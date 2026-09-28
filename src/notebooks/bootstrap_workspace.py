# Databricks notebook source
# MAGIC %md
# MAGIC # FWA Fraud Detection — Workspace Bootstrap
# MAGIC
# MAGIC **One-command post-deploy setup** for the FWA fraud-detection demo. Run this after `databricks bundle deploy`
# MAGIC to provision the Lakebase Autoscaling project, apply UC/warehouse grants for the FWA app service principal,
# MAGIC and seed operational data.
# MAGIC
# MAGIC ### What this notebook does:
# MAGIC 1. Creates Lakebase Autoscaling project with the `fwa_cases` database
# MAGIC 2. Runs DDL schemas and grants PUBLIC access
# MAGIC 3. Seeds fraud investigators
# MAGIC 4. Discovers FWA app service principal and grants:
# MAGIC    - Lakebase: project-level access via permissions API
# MAGIC    - Unity Catalog: USE CATALOG, USE SCHEMA, SELECT on `fe_bar_fwa`
# MAGIC    - SQL Warehouse: CAN_USE permission
# MAGIC    - Serving Endpoints: CAN_QUERY on FWA endpoints
# MAGIC    - Lakebase: PostgreSQL roles for app connections
# MAGIC 5. Creates FWA Genie space with dynamic table references
# MAGIC 6. Seeds FWA investigation cases from gold tables
# MAGIC 7. Creates the empty `fwa_ml_predictions` table for gold MV compatibility
# MAGIC 8. Restarts the FWA app so it re-initializes Lakebase connections and auto-detects Genie spaces
# MAGIC
# MAGIC ### Prerequisites:
# MAGIC - `databricks bundle deploy` has been run (apps exist in workspace)
# MAGIC - Gold tables are populated (run `red_bricks_full_demo` job first, then run this)
# MAGIC - Or run Steps 1-4 before gold tables exist, then re-run Step 5 after pipelines complete

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Unity Catalog Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (leave empty to auto-detect)")
dbutils.widgets.text("lakebase_project_id", "red-bricks-insurance", "Lakebase Project ID")

catalog = dbutils.widgets.get("catalog")
warehouse_id = dbutils.widgets.get("warehouse_id")
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")

print(f"Catalog: {catalog}")
print(f"Warehouse ID: {warehouse_id or '(will auto-detect)'}")

# COMMAND ----------

# MAGIC %pip install psycopg[binary] databricks-sdk "mlflow>=3.14.0" --upgrade --quiet
# MAGIC %restart_python

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"  # SQL-safe quoting (handles hyphens in catalog names)
warehouse_id = dbutils.widgets.get("warehouse_id")
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")

import json
import random
from pathlib import Path

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode
from databricks.sdk.service.postgres import Project, ProjectSpec

random.seed(42)
w = WorkspaceClient()

# Auto-detect SQL warehouse if not provided (prefer RUNNING, fall back to any)
if not warehouse_id.strip():
    all_wh = list(w.warehouses.list())
    running = [wh for wh in all_wh if wh.state and wh.state.value == "RUNNING"]
    if running:
        warehouse_id = running[0].id
        print(f"Auto-detected warehouse (running): {warehouse_id} ({running[0].name})")
    elif all_wh:
        warehouse_id = all_wh[0].id
        print(f"Auto-detected warehouse (state={all_wh[0].state}): {warehouse_id} ({all_wh[0].name})")
    else:
        print("WARNING: No SQL warehouses found. Warehouse grants will be skipped.")

# Resolve paths — works in both bundle-deployed and local contexts
try:
    _here = Path(__file__).resolve().parent
    _repo_root = _here.parent.parent
except NameError:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _ws_root = "/Workspace" + _nb_path.rsplit("/src/notebooks/", 1)[0] if not _nb_path.startswith("/Workspace") else _nb_path.rsplit("/src/notebooks/", 1)[0]
    _repo_root = Path(_ws_root)

print(f"Repo root: {_repo_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Lakebase Autoscaling Project

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

LAKEBASE_PROJECT_ID = lakebase_project_id
LAKEBASE_BRANCH = "production"
LAKEBASE_ENDPOINT_PATH = f"projects/{LAKEBASE_PROJECT_ID}/branches/{LAKEBASE_BRANCH}/endpoints/primary"

LAKEBASE_DATABASES = [
    {
        "database_name": "fwa_cases",
        "schema_file": "src/fwa_lakebase_schema.sql",
        "app_name": "red-bricks-fwa-portal-app",
    },
]

# FWA app that needs UC + warehouse grants
# Auto-discover by listing all apps whose names contain "fwa"
APP_NAME_PATTERNS = []
try:
    for app in w.apps.list():
        name = app.name or ""
        if "fwa" in name.lower():
            APP_NAME_PATTERNS.append(name)
    print(f"Auto-discovered {len(APP_NAME_PATTERNS)} FWA apps: {APP_NAME_PATTERNS}")
except Exception as e:
    print(f"Could not auto-discover apps: {e}. Falling back to known patterns.")
    APP_NAME_PATTERNS = [
        "red-bricks-fwa-portal-app",
    ]

# All UC schemas that the FWA app needs to read
UC_SCHEMAS = [
    "fe_bar_fwa",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Autoscaling Project + Databases + DDL

# COMMAND ----------

import time


def get_or_create_project(project_id: str) -> None:
    """Create a Lakebase Autoscaling project if it doesn't already exist."""
    from databricks.sdk.errors import NotFound
    resource_name = f"projects/{project_id}"
    try:
        project = w.postgres.get_project(name=resource_name)
        print(f"  Project '{project_id}' already exists (uid: {project.uid})")
    except NotFound:
        print(f"  Creating Autoscaling project '{project_id}' (this may take 1-2 min)...")
        project = w.postgres.create_project(
            project=Project(
                spec=ProjectSpec(
                    display_name=project_id,
                    pg_version="17",
                )
            ),
            project_id=project_id,
        ).wait()
        print(f"  Created. UID: {project.uid}")


def ensure_endpoint_awake(endpoint_path: str) -> str:
    """Poll the endpoint until hosts are available (handles scale-to-zero wake-up).

    Returns the resolved host.
    """
    print(f"  Ensuring endpoint is awake: {endpoint_path}")
    max_attempts = 15
    for attempt in range(1, max_attempts + 1):
        ep = w.postgres.get_endpoint(name=endpoint_path)
        if ep.status and ep.status.hosts and ep.status.hosts.host:
            host = ep.status.hosts.host
            print(f"  Endpoint ready: {host}")
            return host
        if attempt < max_attempts:
            wait = min(5 * attempt, 30)
            print(f"  Endpoint not ready (attempt {attempt}/{max_attempts}), retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Endpoint {endpoint_path} did not become ready after {max_attempts} attempts")


def get_pg_connection(project_id: str, database_name: str) -> psycopg.Connection:
    """Get a psycopg connection to a Lakebase Autoscaling database."""
    endpoint_path = f"projects/{project_id}/branches/{LAKEBASE_BRANCH}/endpoints/primary"
    ep = w.postgres.get_endpoint(name=endpoint_path)
    host = ep.status.hosts.host
    cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
    return psycopg.connect(
        f"host={host} "
        f"dbname={database_name} "
        f"user={w.current_user.me().user_name} "
        f"password={cred.token} "
        f"sslmode=require"
    )


def create_database(project_id: str, database_name: str) -> None:
    """Create the database if it doesn't exist (connects to 'databricks_postgres' first)."""
    conn = get_pg_connection(project_id, "databricks_postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {database_name}")
            print(f"  Database '{database_name}' created.")
        else:
            print(f"  Database '{database_name}' already exists.")
    conn.close()


def run_ddl(project_id: str, database_name: str, schema_file: str) -> None:
    """Run the DDL schema SQL file against the database."""
    schema_path = _repo_root / schema_file
    ddl = schema_path.read_text()
    print(f"  Running DDL from {schema_file}...")
    with get_pg_connection(project_id, database_name) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print(f"  DDL applied successfully.")


def grant_public_access(project_id: str, database_name: str) -> None:
    """Grant PUBLIC access so app service principals can connect."""
    print(f"  Granting PUBLIC access on {database_name}...")
    with get_pg_connection(project_id, database_name) as conn:
        with conn.cursor() as cur:
            cur.execute("GRANT ALL ON ALL TABLES IN SCHEMA public TO PUBLIC")
            cur.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO PUBLIC")
            cur.execute("GRANT USAGE ON SCHEMA public TO PUBLIC")
            cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO PUBLIC")
            cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO PUBLIC")
        conn.commit()
    print(f"  PUBLIC access granted.")


def create_lakebase_roles_for_sps(project_id: str, sp_client_ids: list[str]) -> None:
    """Create Lakebase-managed OAuth roles for app service principals.

    Uses the SDK postgres.create_role API to register SPs with LAKEBASE_OAUTH_V1
    auth. This is required for SPs to generate valid database credentials —
    raw PostgreSQL CREATE ROLE results in NO_LOGIN auth which rejects OAuth tokens.

    On clean redeploys, apps get new SPs. This function deletes stale roles from
    previous deploys before creating roles for the current SPs.
    """
    import time
    from databricks.sdk.service.postgres import Role, RoleRoleSpec, RoleAuthMethod, RoleIdentityType

    if not sp_client_ids:
        return

    branch = f"projects/{project_id}/branches/{LAKEBASE_BRANCH}"

    # Snapshot all existing roles on this branch
    existing_roles = list(w.postgres.list_roles(parent=branch))
    existing_by_pg_role = {r.status.postgres_role: r for r in existing_roles}
    current_sp_set = set(sp_client_ids)

    # --- Clean up orphaned roles from previous deploys ---
    # On a clean redeploy, old SPs are gone but their Lakebase roles persist.
    # These stale roles also block role_id reuse, so delete them first.
    for role in existing_roles:
        pg_role = role.status.postgres_role
        role_name = role.name or ""
        if pg_role not in current_sp_set and "/roles/sp-" in role_name:
            print(f"    Deleting stale role for old SP: {pg_role} ({role_name})")
            try:
                w.postgres.delete_role(name=role_name)
                time.sleep(5)
            except Exception as e:
                print(f"    Could not delete stale role: {e}")

    # --- Create / verify roles for current SPs ---
    print(f"  Creating Lakebase OAuth roles for {len(sp_client_ids)} app SPs...")
    for sp_id in sp_client_ids:
        if sp_id in existing_by_pg_role:
            role = existing_by_pg_role[sp_id]
            if role.status.auth_method == RoleAuthMethod.LAKEBASE_OAUTH_V1:
                print(f"    {sp_id}: OAuth role already exists")
                continue
            # Delete NO_LOGIN role so we can recreate with OAuth
            print(f"    {sp_id}: Deleting NO_LOGIN role...")
            w.postgres.delete_role(name=role.name)
            time.sleep(8)

        # Derive role_id from SP UUID — deterministic and avoids index-based collisions
        # Pattern: ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$
        role_id = f"sp-{sp_id}"
        try:
            op = w.postgres.create_role(
                parent=branch,
                role=Role(
                    spec=RoleRoleSpec(
                        postgres_role=sp_id,
                        auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                        identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                    )
                ),
                role_id=role_id,
            )
            result = op.wait()
            print(f"    {sp_id}: OAuth role created ({result.name})")
            time.sleep(5)
        except Exception as e:
            print(f"    {sp_id}: Role creation failed: {e}")


def grant_lakebase_access_to_sps(project_id: str, sp_infos: list[dict]) -> None:
    """Grant SP access to the Autoscaling project via permissions API.

    Uses the database-projects permissions endpoint with the project UID.
    Permission level CAN_USE allows SPs to generate credentials and connect.
    """
    if not sp_infos:
        return

    import requests
    host = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Resolve project UID — the permissions API requires the UID, not the display name
    project = w.postgres.get_project(name=f"projects/{project_id}")
    project_uid = project.uid
    print(f"  Project UID: {project_uid}")

    # Build ACL with all SPs in a single PATCH
    acl = [
        {"service_principal_name": sp["sp_name"], "permission_level": "CAN_USE"}
        for sp in sp_infos
    ]

    print(f"  Granting CAN_USE on database-projects/{project_uid} to {len(acl)} app SPs...")
    resp = requests.patch(
        f"https://{host}/api/2.0/permissions/database-projects/{project_uid}",
        headers=headers,
        json={"access_control_list": acl},
    )
    if resp.status_code == 200:
        for sp in sp_infos:
            print(f"    {sp['app_name']}: CAN_USE granted")
    else:
        print(f"  Permission grant failed ({resp.status_code}): {resp.text[:300]}")
        print(f"  Grant manually: Workspace Settings → Lakebase → {project_id} → Permissions")

# COMMAND ----------

print("=" * 60)
print("STEP 1: Lakebase Autoscaling Project")
print("=" * 60)

# 1. Create project (single LRO)
print(f"\n--- Project: {LAKEBASE_PROJECT_ID} ---")
get_or_create_project(LAKEBASE_PROJECT_ID)

# 2. Ensure endpoint is awake
endpoint_host = ensure_endpoint_awake(LAKEBASE_ENDPOINT_PATH)

# 3. Create databases, run DDL, grant access
for cfg in LAKEBASE_DATABASES:
    print(f"\n--- Database: {cfg['database_name']} ---")
    create_database(LAKEBASE_PROJECT_ID, cfg["database_name"])
    run_ddl(LAKEBASE_PROJECT_ID, cfg["database_name"], cfg["schema_file"])
    grant_public_access(LAKEBASE_PROJECT_ID, cfg["database_name"])

print("\nLakebase Autoscaling project ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Seed Care Managers & Fraud Investigators

# COMMAND ----------

FRAUD_INVESTIGATORS = [
    ("karen.mitchell@redbricks.example.com", "Karen Mitchell", "SIU Analyst", "Special Investigations Unit", 30),
    ("robert.chen@redbricks.example.com", "Robert Chen", "SIU Analyst", "Special Investigations Unit", 30),
    ("diana.torres@redbricks.example.com", "Diana Torres", "SIU Manager", "Special Investigations Unit", 20),
    ("mark.anderson@redbricks.example.com", "Mark Anderson", "Clinical Reviewer", "Payment Integrity", 25),
    ("jennifer.wong@redbricks.example.com", "Jennifer Wong", "Legal Counsel", "Legal & Compliance", 15),
    ("steven.patel@redbricks.example.com", "Steven Patel", "Recovery Specialist", "Payment Integrity", 35),
]

# COMMAND ----------

print("=" * 60)
print("STEP 2: Seed Fraud Investigators")
print("=" * 60)

# FWA Portal — Fraud Investigators
print("\nSeeding fraud investigators...")
with get_pg_connection(LAKEBASE_PROJECT_ID, "fwa_cases") as conn:
    with conn.cursor() as cur:
        for email, name, role, dept, caseload in FRAUD_INVESTIGATORS:
            cur.execute(
                """INSERT INTO fraud_investigators (email, display_name, role, department, max_caseload)
                   VALUES (%s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING""",
                (email, name, role, dept, caseload),
            )
    conn.commit()
print(f"  {len(FRAUD_INVESTIGATORS)} fraud investigators seeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Discover App Service Principals & Grant Permissions

# COMMAND ----------

def discover_app_service_principals() -> list[dict]:
    """Discover service principals for all deployed apps.

    Returns the service_principal_client_id (UUID / application_id) as sp_name,
    which is the identifier accepted by both SQL GRANT and REST API permissions.
    """
    sps = []
    for app_name in APP_NAME_PATTERNS:
        try:
            app = w.apps.get(app_name)
            sp_id = getattr(app, "service_principal_id", None) or getattr(app, "effective_service_principal_id", None)

            # Prefer service_principal_client_id (UUID) — this is the application_id
            # that SQL GRANT and REST API permissions both accept.
            sp_client_id = getattr(app, "service_principal_client_id", None)

            # Fallback: look up the SP by numeric ID to get its application_id
            if not sp_client_id and sp_id:
                try:
                    sp_obj = w.service_principals.get(sp_id)
                    sp_client_id = sp_obj.application_id
                except Exception:
                    pass

            # Last resort: search by display name
            if not sp_client_id:
                all_sps = list(w.service_principals.list(filter=f"displayName co \"{app_name}\""))
                if all_sps:
                    sp_client_id = all_sps[0].application_id
                    sp_id = sp_id or all_sps[0].id

            if sp_client_id:
                sps.append({"app_name": app_name, "sp_name": sp_client_id, "sp_id": sp_id})
                print(f"  {app_name}: SP client_id={sp_client_id} (ID={sp_id})")
            else:
                print(f"  {app_name}: WARNING — could not discover service principal")
        except Exception as e:
            print(f"  {app_name}: not found ({e})")
    return sps


def discover_serving_endpoint_service_principals() -> list[dict]:
    """Discover service principals for custom model serving endpoints.

    Serving endpoints that run agent models (e.g. fwa-supervisor-agent) get their
    own SPs which need the same UC, warehouse, Genie, and Lakebase grants as app SPs.
    """
    # FWA serving endpoints that need grants (exclude FMAPI pay-per-token endpoints)
    SERVING_ENDPOINT_PATTERNS = ["fwa-supervisor-agent", "fwa-fraud-scorer"]

    import requests
    host = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}"}

    sps = []
    for ep_name in SERVING_ENDPOINT_PATTERNS:
        try:
            resp = requests.get(f"https://{host}/api/2.0/serving-endpoints/{ep_name}", headers=headers)
            if resp.status_code != 200:
                print(f"  {ep_name}: not found ({resp.status_code})")
                continue

            ep_data = resp.json()
            found_sp = None

            # Strategy 1: Search by display name (SDK-created endpoints)
            sp_filter = f"displayName co \"{ep_name}\""
            matching_sps = list(w.service_principals.list(filter=sp_filter))
            if matching_sps:
                found_sp = matching_sps[0]

            # Strategy 2: Check endpoint events for system SP creation.
            # The events API logs "System service principal creation with ID `<uuid>`"
            # for each config version. We want the most recent one.
            if not found_sp:
                try:
                    import re as _re_events
                    events_resp = requests.get(
                        f"https://{host}/api/2.0/serving-endpoints/{ep_name}/events",
                        headers=headers,
                    )
                    if events_resp.status_code == 200:
                        sp_uuid_pattern = _re_events.compile(
                            r"System service principal creation with ID `([0-9a-f-]{36})`"
                        )
                        # Events are chronological; iterate in reverse to get the latest SP
                        for evt in reversed(events_resp.json().get("events", [])):
                            m = sp_uuid_pattern.search(evt.get("message", ""))
                            if m:
                                evt_sp_uuid = m.group(1)
                                evt_sps = list(w.service_principals.list(
                                    filter=f"applicationId eq \"{evt_sp_uuid}\""
                                ))
                                if evt_sps:
                                    found_sp = evt_sps[0]
                                    print(f"  {ep_name}: discovered runtime SP via endpoint events")
                                break
                except Exception as e_evt:
                    print(f"  {ep_name}: endpoint events fallback failed: {e_evt}")

            # Strategy 3: Check warehouse query history for the endpoint's runtime SP.
            # UI-created endpoints get a system-managed SP with a UUID name that doesn't
            # match the endpoint name. The SP shows up in SQL query history when the model
            # executes queries via WorkspaceClient().
            if not found_sp:
                try:
                    wh_id = spark.conf.get("spark.databricks.warehouseId", "")
                    if not wh_id:
                        wh_id = [wh.id for wh in w.warehouses.list()][0] if list(w.warehouses.list()) else ""
                    if wh_id:
                        qh_resp = requests.get(
                            f"https://{host}/api/2.0/sql/history/queries",
                            headers=headers,
                            params={
                                "max_results": "20",
                                "include_metrics": "false",
                                "filter_by.warehouse_ids": wh_id,
                            },
                        )
                        if qh_resp.status_code == 200:
                            # Collect SP-like user_names (UUID format) that ran queries
                            import re
                            uuid_pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
                            known_app_sps = {sp_info["sp_name"] for sp_info in app_sps} if "app_sps" in dir() else set()
                            candidate_sps = set()
                            for q in qh_resp.json().get("res", []):
                                user = q.get("user_name", "")
                                if uuid_pattern.match(user) and user not in known_app_sps:
                                    candidate_sps.add(user)
                            # Match candidates against SCIM to find the endpoint's SP
                            for cand in candidate_sps:
                                cand_sps = list(w.service_principals.list(filter=f"applicationId eq \"{cand}\""))
                                if cand_sps:
                                    sp_obj = cand_sps[0]
                                    # Accept if display name contains serving-related keywords or is unrecognized
                                    display = (sp_obj.display_name or "").lower()
                                    if "app-" not in display and "vending" not in display:
                                        found_sp = sp_obj
                                        print(f"  {ep_name}: discovered runtime SP via query history")
                                        break
                except Exception as e2:
                    print(f"  {ep_name}: query history fallback failed: {e2}")

            if found_sp:
                sps.append({"app_name": f"endpoint:{ep_name}", "sp_name": found_sp.application_id, "sp_id": found_sp.id})
                print(f"  {ep_name}: SP client_id={found_sp.application_id} (ID={found_sp.id}, name={found_sp.display_name})")
            else:
                print(f"  {ep_name}: WARNING — endpoint exists but no matching SP found")
                print(f"    Try granting manually: look up SP in workspace Settings → Identity → Service Principals")
        except Exception as e:
            print(f"  {ep_name}: error ({e})")
    return sps

# COMMAND ----------

print("=" * 60)
print("STEP 3: Discover App & Serving Endpoint Service Principals")
print("=" * 60)

print("\n--- App SPs ---")
app_sps = discover_app_service_principals()

print("\n--- Serving Endpoint SPs ---")
endpoint_sps = discover_serving_endpoint_service_principals()

# Merge — deduplicate by sp_name (client_id)
existing_sp_names = {sp["sp_name"] for sp in app_sps}
for sp in endpoint_sps:
    if sp["sp_name"] not in existing_sp_names:
        app_sps.append(sp)
        existing_sp_names.add(sp["sp_name"])
    else:
        print(f"  {sp['app_name']}: already in app_sps, skipping duplicate")

print(f"\nTotal SPs to grant: {len(app_sps)}")

if not app_sps:
    print("\nNo service principals found. Run 'databricks bundle deploy' first.")
    print("Skipping grant steps.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Lakebase Roles for App Service Principals

# COMMAND ----------

# Grant SPs project-level access (replaces DAB security labels)
if app_sps:
    print("\n--- Lakebase Project Access ---")
    grant_lakebase_access_to_sps(LAKEBASE_PROJECT_ID, app_sps)

# Apps connect to Lakebase using their SP client_id (UUID) as the PostgreSQL username.
# Lakebase OAuth roles are created at the branch level (not per-database).
# The SDK's create_role registers the SP with LAKEBASE_OAUTH_V1 auth method.
if app_sps:
    sp_client_ids = [sp["sp_name"] for sp in app_sps]
    create_lakebase_roles_for_sps(LAKEBASE_PROJECT_ID, sp_client_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant Unity Catalog Permissions

# COMMAND ----------

if app_sps:
    print("Granting Unity Catalog permissions...\n")
    for sp_info in app_sps:
        sp_name = sp_info["sp_name"]
        app_name = sp_info["app_name"]
        print(f"  --- {app_name} (SP: {sp_name}) ---")

        # USE CATALOG + BROWSE (BROWSE enables catalog/warehouse auto-detection in apps)
        for priv in ["USE CATALOG", "BROWSE"]:
            try:
                spark.sql(f"GRANT {priv} ON CATALOG {catalog_sql} TO `{sp_name}`")
                print(f"    {priv} on {catalog}")
            except Exception as e:
                print(f"    {priv}: {e}")

        # USE SCHEMA + SELECT on each domain schema
        for schema in UC_SCHEMAS:
            try:
                spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_sql}.{schema} TO `{sp_name}`")
                spark.sql(f"GRANT SELECT ON SCHEMA {catalog_sql}.{schema} TO `{sp_name}`")
                print(f"    USE SCHEMA + SELECT on {catalog}.{schema}")
            except Exception as e:
                print(f"    {catalog}.{schema}: {e}")

        # EXECUTE on ai_tools schema functions (used by the Care Intelligence Agent)
        try:
            spark.sql(f"GRANT EXECUTE ON SCHEMA {catalog_sql}.ai_tools TO `{sp_name}`")
            print(f"    EXECUTE on {catalog}.ai_tools functions")
        except Exception as e:
            print(f"    EXECUTE on ai_tools: {e}")
        print()

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant SQL Warehouse Permissions

# COMMAND ----------

if app_sps and warehouse_id:
    print(f"Granting CAN_USE on warehouse {warehouse_id}...\n")
    import requests

    host = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    for sp_info in app_sps:
        sp_name = sp_info["sp_name"]
        app_name = sp_info["app_name"]

        resp = requests.patch(
            f"https://{host}/api/2.0/permissions/sql/warehouses/{warehouse_id}",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "access_control_list": [
                    {"service_principal_name": sp_name, "permission_level": "CAN_USE"}
                ]
            },
        )
        if resp.status_code == 200:
            print(f"  {app_name}: CAN_USE granted")
        else:
            print(f"  {app_name}: {resp.status_code} — {resp.text}")
elif app_sps and not warehouse_id:
    print("No warehouse_id provided — skipping warehouse grants.")
    print("Set the warehouse_id widget and re-run this cell if needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant Serving Endpoint & Vector Search Permissions

# COMMAND ----------

if app_sps:
    import requests as _req_ep
    _host_ep = spark.conf.get("spark.databricks.workspaceUrl")
    _token_ep = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    _ep_headers = {"Authorization": f"Bearer {_token_ep}", "Content-Type": "application/json"}

    # Serving endpoints that FWA app SP needs CAN_QUERY on.
    # Includes the FWA supervisor agent and the fraud scorer.
    CUSTOM_ENDPOINTS = ["fwa-supervisor-agent", "fwa-fraud-scorer"]

    for ep_name in CUSTOM_ENDPOINTS:
        print(f"\nGranting CAN_QUERY on serving endpoint '{ep_name}'...")
        for sp_info in app_sps:
            resp = _req_ep.patch(
                f"https://{_host_ep}/api/2.0/permissions/serving-endpoints/{ep_name}",
                headers=_ep_headers,
                json={
                    "access_control_list": [
                        {"service_principal_name": sp_info["sp_name"], "permission_level": "CAN_QUERY"}
                    ]
                },
            )
            if resp.status_code == 200:
                print(f"  {sp_info['app_name']}: CAN_QUERY granted")
            else:
                print(f"  {sp_info['app_name']}: {resp.status_code} — {resp.text[:200]}")

    # Vector Search — grant CAN_USE on the endpoint
    VS_ENDPOINT_NAME = "red-bricks-vs-endpoint"

    # Look up the endpoint UUID — Azure requires UUID in the permissions URL path
    _vs_endpoint_id = None
    try:
        _vs_resp = _req_ep.get(
            f"https://{_host_ep}/api/2.0/vector-search/endpoints/{VS_ENDPOINT_NAME}",
            headers=_ep_headers,
        )
        if _vs_resp.status_code == 200:
            _vs_endpoint_id = _vs_resp.json().get("id", VS_ENDPOINT_NAME)
        else:
            _vs_endpoint_id = VS_ENDPOINT_NAME  # fall back to name (works on AWS)
    except Exception:
        _vs_endpoint_id = VS_ENDPOINT_NAME

    print(f"\nGranting vector search permissions (endpoint id={_vs_endpoint_id})...")
    for sp_info in app_sps:
        sp_name = sp_info["sp_name"]
        # Grant CAN_USE on the vector search endpoint
        resp = _req_ep.patch(
            f"https://{_host_ep}/api/2.0/permissions/vector-search-endpoints/{_vs_endpoint_id}",
            headers=_ep_headers,
            json={
                "access_control_list": [
                    {"service_principal_name": sp_name, "permission_level": "CAN_USE"}
                ]
            },
        )
        if resp.status_code == 200:
            print(f"  {sp_info['app_name']}: CAN_USE on VS endpoint granted")
        else:
            print(f"  {sp_info['app_name']}: VS endpoint — {resp.status_code} — {resp.text[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Genie Spaces & Grant Permissions

# COMMAND ----------

import requests as _requests

_host = spark.conf.get("spark.databricks.workspaceUrl")
_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
_genie_headers = {"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}
_current_user = w.current_user.me().user_name

# Define FWA Genie spaces — tables are dynamic based on catalog
GENIE_SPACE_CONFIGS = [
    {
        "title": "FWA — Fraud Detection & Investigation",
        "description": "Fraud, waste, and abuse analytics: provider risk scores, flagged claims, investigation cases, and network analysis.",
        "tables": sorted([
            f"{catalog}.fe_bar_fwa.gold_fwa_provider_risk",
            f"{catalog}.fe_bar_fwa.gold_fwa_claim_flags",
            f"{catalog}.fe_bar_fwa.gold_fwa_summary",
            f"{catalog}.fe_bar_fwa.silver_fwa_signals",
            f"{catalog}.fe_bar_fwa.silver_fwa_investigation_cases",
            f"{catalog}.fe_bar_fwa.gold_fwa_member_risk",
            f"{catalog}.fe_bar_fwa.gold_fwa_network_analysis",
            f"{catalog}.fe_bar_fwa.silver_claims_medical",
            f"{catalog}.fe_bar_fwa.silver_enrollment",
            f"{catalog}.fe_bar_fwa.silver_providers",
        ]),
    },
    {
        "title": "FWA AI Ops — Agent Observability",
        "description": "Observability for the FWA Investigation Agent system. Query token consumption, cost estimates, request volumes, inference table logs (every agent request/response automatically captured), evaluation scores, ML predictions, and AI classifications across Llama 4 Maverick (supervisor), Claude Haiku 4.5 (analyst), and the XGBoost fraud scorer. Filter to FWA endpoints: fwa-fraud-scorer and fwa-supervisor-agent. Cost rates: Llama $0.40/$1.60 per 1M input/output tokens, Claude Haiku 4.5 $1.00/$5.00 per 1M. The fwa_supervisor_payload table contains every request/response to the FWA agent endpoint with execution_time_ms, status_code, and full request/response JSON.",
        "tables": sorted([
            "system.serving.endpoint_usage",
            "system.serving.served_entities",
            f"{catalog}.fe_bar_fwa.fwa_agent_evaluation_results",
            f"{catalog}.fe_bar_fwa.fwa_agent_otel_annotations",
            f"{catalog}.fe_bar_fwa.fwa_agent_otel_logs",
            f"{catalog}.fe_bar_fwa.fwa_agent_otel_spans",
            f"{catalog}.fe_bar_fwa.fwa_ml_predictions",
            f"{catalog}.fe_bar_fwa.fwa_model_inference",
            f"{catalog}.fe_bar_fwa.fwa_supervisor_payload",
            f"{catalog}.fe_bar_fwa.gold_fwa_ai_classification",
            f"{catalog}.fe_bar_fwa.gold_fwa_model_scores",
        ]),
    },
]


def create_or_get_genie_space(title: str, description: str, tables: list[str]) -> str | None:
    """Create a Genie space if one with the same title doesn't already exist. Returns space_id."""
    # Check if a space with this title already exists
    try:
        existing = _requests.get(
            f"https://{_host}/api/2.0/genie/spaces",
            headers=_genie_headers,
        ).json().get("spaces", [])
        for s in existing:
            if s.get("title") == title:
                print(f"  Already exists: {s['space_id']}")
                return s["space_id"]
    except Exception:
        pass

    # Filter tables to only those that actually exist in the catalog
    valid_tables = []
    for t in tables:
        try:
            # Backtick-quote each part for catalogs with special chars (e.g. hyphens)
            quoted = ".".join(f"`{part}`" for part in t.split("."))
            spark.sql(f"DESCRIBE TABLE {quoted}")
            valid_tables.append(t)
        except Exception:
            print(f"  Skipping missing table: {t}")

    if not valid_tables:
        print("  No valid tables found — skipping space creation.")
        return None

    serialized = json.dumps({
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": t} for t in sorted(valid_tables)]
        }
    })

    try:
        resp = _requests.post(
            f"https://{_host}/api/2.0/genie/spaces",
            headers=_genie_headers,
            json={
                "warehouse_id": warehouse_id,
                "serialized_space": serialized,
                "title": title,
                "description": description,
            },
        )
        if resp.status_code == 200:
            space_id = resp.json().get("space_id")
            print(f"  Created: {space_id}")
            return space_id
        else:
            print(f"  Creation failed ({resp.status_code}): {resp.text[:300]}")
    except Exception as e:
        print(f"  Creation failed: {e}")
    return None


def grant_genie_permissions(space_id: str, sp_names: list[str]) -> None:
    """Grant CAN_RUN on a Genie space to all app and serving endpoint service principals.

    Grants permissions at TWO levels:
    1. Genie API level (/permissions/genie/{space_id}) — controls Genie API access
    2. Workspace-objects level (/permissions/workspace-objects/{obj_id}) — controls
       workspace ACL traversal required by serving endpoint runtime SPs
    """
    acl = [{"user_name": _current_user, "permission_level": "CAN_MANAGE"}]
    for sp_name in sp_names:
        acl.append({"service_principal_name": sp_name, "permission_level": "CAN_RUN"})

    # 1. Genie API permissions
    resp = _requests.put(
        f"https://{_host}/api/2.0/permissions/genie/{space_id}",
        headers=_genie_headers,
        json={"access_control_list": acl},
    )
    if resp.status_code == 200:
        print(f"  Genie API permissions granted to {len(sp_names)} SPs")
    else:
        print(f"  Genie API permission grant failed ({resp.status_code}): {resp.text[:200]}")

    # 2. Workspace-objects permissions (required for serving endpoint runtime SPs)
    # The Genie permissions response contains the workspace object ID in object_id field
    try:
        perm_resp = _requests.get(
            f"https://{_host}/api/2.0/permissions/genie/{space_id}",
            headers=_genie_headers,
        )
        if perm_resp.status_code == 200:
            obj_path = perm_resp.json().get("object_id", "")  # e.g. "/genie/4095602807544805"
            ws_obj_id = obj_path.split("/")[-1] if "/" in obj_path else ""
            if ws_obj_id:
                ws_acl = [{"service_principal_name": sp, "permission_level": "CAN_RUN"} for sp in sp_names]
                ws_resp = _requests.patch(
                    f"https://{_host}/api/2.0/permissions/workspace-objects/{ws_obj_id}",
                    headers=_genie_headers,
                    json={"access_control_list": ws_acl},
                )
                if ws_resp.status_code == 200:
                    print(f"  Workspace-objects ACL granted (object_id={ws_obj_id})")
                else:
                    print(f"  Workspace-objects ACL failed ({ws_resp.status_code}): {ws_resp.text[:200]}")
    except Exception as e:
        print(f"  Workspace-objects ACL grant failed: {e}")


# COMMAND ----------

print("Creating Genie spaces and granting permissions...\n")

sp_names_for_genie = [sp["sp_name"] for sp in app_sps] if app_sps else []

for cfg in GENIE_SPACE_CONFIGS:
    print(f"--- {cfg['title']} ---")
    if not warehouse_id:
        print("  Skipped — no warehouse_id available")
        continue

    space_id = create_or_get_genie_space(cfg["title"], cfg["description"], cfg["tables"])

    if space_id and sp_names_for_genie:
        grant_genie_permissions(space_id, sp_names_for_genie)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable AI Gateway Inference Tables on Serving Endpoints
# MAGIC Endpoint config updates reset AI Gateway settings, so this re-enables
# MAGIC inference table logging on every bootstrap run.

# COMMAND ----------

import requests as _gw_requests

_gw_host = spark.conf.get("spark.databricks.workspaceUrl")
_gw_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
_gw_headers = {"Authorization": f"Bearer {_gw_token}", "Content-Type": "application/json"}

_ai_gateway_endpoints = {
    "fwa-supervisor-agent": {"catalog_name": catalog, "schema_name": "analytics", "table_name_prefix": "fwa_supervisor"},
}

print("\n--- AI Gateway Inference Tables ---")
for _ep_name, _itc in _ai_gateway_endpoints.items():
    _ep_check = _gw_requests.get(f"https://{_gw_host}/api/2.0/serving-endpoints/{_ep_name}", headers=_gw_headers)
    if _ep_check.status_code != 200:
        print(f"  {_ep_name}: not found, skipping")
        continue
    _gw_resp = _gw_requests.put(
        f"https://{_gw_host}/api/2.0/serving-endpoints/{_ep_name}/ai-gateway",
        headers=_gw_headers,
        json={"inference_table_config": {**_itc, "enabled": True}},
    )
    if _gw_resp.status_code == 200:
        print(f"  {_ep_name}: enabled -> {_itc['catalog_name']}.{_itc['schema_name']}.{_itc['table_name_prefix']}_payload")
    else:
        print(f"  {_ep_name}: WARNING ({_gw_resp.status_code}): {_gw_resp.text[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Provision MLflow UC Trace Storage for the FWA Agent
# MAGIC
# MAGIC Links a dedicated MLflow experiment to Unity Catalog OTel tables so the FWA
# MAGIC Investigation app streams agent traces (supervisor + genie + gemini spans)
# MAGIC into `{catalog}.fe_bar_fwa.fwa_agent_otel_*` in real-time. Uses the modern
# MAGIC `mlflow.set_experiment(trace_location=UnityCatalog(...))` API, which creates
# MAGIC the backing tables server-side on first call and is idempotent thereafter.
# MAGIC
# MAGIC **Important:** the experiment name MUST be one that has never had legacy
# MAGIC `databricksTrace*StorageTable` tags set on it. If such a tag exists,
# MAGIC `set_experiment` early-returns and silently skips table creation (there is
# MAGIC no supported API to delete experiment tags on Databricks-managed MLflow).

# COMMAND ----------

print("=" * 60)
print("STEP 3b: Provision MLflow UC Trace Storage (FWA Agent)")
print("=" * 60)

# These MUST stay in sync with app-fwa/backend/env_config.py defaults and the
# MLFLOW_UC_EXPERIMENT / UC_TRACE_* env vars in resources/app_fwa.yml.
TRACE_EXPERIMENT = "/Shared/red-bricks-fwa-agent-traces-uc2"
TRACE_SCHEMA = "fe_bar_fwa"
TRACE_TABLE_PREFIX = "fwa_agent"

if not warehouse_id:
    print("  Skipped — no warehouse_id available (required to provision UC trace tables).")
else:
    try:
        import os as _os
        import mlflow
        from mlflow.entities import UnityCatalog

        mlflow.set_tracking_uri("databricks")
        # The link/provision call needs a warehouse to create the Delta tables.
        _os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{TRACE_SCHEMA}")

        exp = mlflow.set_experiment(
            TRACE_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=catalog,
                schema_name=TRACE_SCHEMA,
                table_prefix=TRACE_TABLE_PREFIX,
            ),
        )
        spans_table = f"{catalog}.{TRACE_SCHEMA}.{TRACE_TABLE_PREFIX}_otel_spans"
        print(f"  Experiment linked: {TRACE_EXPERIMENT} (ID: {exp.experiment_id})")
        print(f"  Resolved location: {exp.trace_location}")
        print(f"  Spans table:       {spans_table}")

        # Verify the OTel tables were actually created (guards against the
        # silent early-return failure mode described above).
        _otel_tables = [
            f"{TRACE_TABLE_PREFIX}_otel_spans",
            f"{TRACE_TABLE_PREFIX}_otel_logs",
            f"{TRACE_TABLE_PREFIX}_otel_annotations",
        ]
        _existing = {
            r["tableName"]
            for r in spark.sql(
                f"SHOW TABLES IN {catalog_sql}.{TRACE_SCHEMA} LIKE '{TRACE_TABLE_PREFIX}_otel_*'"
            ).collect()
        }
        _missing = [t for t in _otel_tables if t not in _existing]
        if _missing:
            print(
                f"  WARNING: expected OTel tables missing: {_missing}. "
                "The experiment may be linked to a stale destination. "
                f"Choose a fresh TRACE_EXPERIMENT name and re-run."
            )
        else:
            print(f"  Verified OTel tables exist: {sorted(_existing)}")

        # Grant the app service principals write + read on the trace tables so
        # the running app can export spans (bootstrap creates them as the
        # notebook runner, not as the app SP).
        if app_sps:
            print("\n  Granting app SPs access to trace tables...")
            for _sp in app_sps:
                _sp_name = _sp["sp_name"]
                for _tbl in sorted(_existing):
                    _fq = f"{catalog_sql}.{TRACE_SCHEMA}.`{_tbl}`"
                    try:
                        spark.sql(f"GRANT SELECT, MODIFY ON TABLE {_fq} TO `{_sp_name}`")
                    except Exception as _ge:
                        print(f"    {_sp_name} on {_tbl}: {_ge}")
            print(f"    SELECT + MODIFY granted to {len(app_sps)} SP(s) on {len(_existing)} table(s)")
    except Exception as _te:
        import traceback as _tb
        print(f"  WARNING: UC trace storage provisioning failed: {_te}")
        _tb.print_exc()

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Empty ML Predictions Table

# COMMAND ----------

print("=" * 60)
print("STEP 4: Pre-create ML Predictions Table")
print("=" * 60)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.analytics")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_sql}.fe_bar_fwa.fwa_ml_predictions (
        claim_id STRING,
        ml_fraud_probability DOUBLE,
        ml_risk_tier STRING,
        model_version STRING,
        scored_at STRING
    ) USING DELTA
""")
print(f"  {catalog}.fe_bar_fwa.fwa_ml_predictions — ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed Operational Data from Gold Tables
# MAGIC
# MAGIC **Run this step after pipelines have completed** — it reads from gold tables to
# MAGIC populate Lakebase with FWA investigation cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### FWA Investigations

# COMMAND ----------

# MAGIC %md
# MAGIC ### FWA Investigations

# COMMAND ----------

def seed_fwa_investigations() -> int:
    """Seed FWA investigation cases from silver/gold tables into Lakebase."""

    # Get investigator IDs
    investigators = []
    with get_pg_connection(LAKEBASE_PROJECT_ID, "fwa_cases") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT investigator_id, display_name FROM fraud_investigators WHERE is_active = TRUE")
            investigators = cur.fetchall()

    if not investigators:
        print("  WARNING: No investigators found. Seed investigators first.")
        return 0

    # Read investigation cases
    try:
        cases_df = spark.sql(f"""
            SELECT investigation_id, investigation_type, target_type, target_id, target_name,
                   fraud_types, severity, status, estimated_overpayment, claims_involved_count,
                   investigation_summary, evidence_summary, rules_risk_score, ml_risk_score,
                   created_date
            FROM {catalog_sql}.fe_bar_fwa.silver_fwa_investigation_cases
            ORDER BY rules_risk_score DESC
        """).collect()
    except Exception as e:
        print(f"  silver_fwa_investigation_cases not available: {e}")
        return 0

    count = 0
    with get_pg_connection(LAKEBASE_PROJECT_ID, "fwa_cases") as conn:
        with conn.cursor() as cur:
            for row in cases_df:
                assigned_id = None
                if row.status != "Open":
                    inv = investigators[count % len(investigators)]
                    assigned_id = str(inv[0])

                rules_score = row.rules_risk_score or 0.5
                ml_score = row.ml_risk_score or 0.5
                composite = round(0.6 * rules_score + 0.4 * ml_score, 3)
                est_overpayment = float(row.estimated_overpayment or 0)

                # Compute confirmed/recovered for closed cases
                confirmed_overpayment = None
                recovered_amount = 0
                if row.status == "Closed — Confirmed Fraud":
                    confirmed_overpayment = round(est_overpayment * random.uniform(0.70, 1.10), 2)
                    recovered_amount = round(confirmed_overpayment * random.uniform(0.40, 0.85), 2)
                elif row.status == "Recovery In Progress":
                    confirmed_overpayment = round(est_overpayment * random.uniform(0.75, 1.05), 2)
                    recovered_amount = round(confirmed_overpayment * random.uniform(0.10, 0.45), 2)
                elif row.status == "Closed — No Fraud":
                    confirmed_overpayment = 0
                    recovered_amount = 0
                elif row.status == "Closed — Insufficient Evidence":
                    confirmed_overpayment = round(est_overpayment * random.uniform(0.20, 0.50), 2)
                    recovered_amount = 0

                cur.execute(
                    """INSERT INTO fwa_investigations (
                        investigation_id, investigation_type, target_type, target_id, target_name,
                        fraud_types, severity, source, status, assigned_investigator_id,
                        estimated_overpayment, confirmed_overpayment, recovered_amount,
                        claims_involved_count,
                        investigation_summary, evidence_summary,
                        rules_risk_score, ml_risk_score, composite_risk_score,
                        created_at
                    ) VALUES (
                        %s, %s::investigation_type, %s, %s, %s,
                        %s, %s::fraud_severity, 'Rules Engine'::investigation_source,
                        %s::investigation_status, CAST(%s AS uuid),
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s::timestamptz
                    ) ON CONFLICT (investigation_id) DO NOTHING""",
                    (row.investigation_id, row.investigation_type, row.target_type,
                     row.target_id, row.target_name,
                     row.fraud_types.split(",") if row.fraud_types else [],
                     row.severity, row.status, assigned_id,
                     est_overpayment, confirmed_overpayment, recovered_amount,
                     row.claims_involved_count,
                     row.investigation_summary, row.evidence_summary,
                     rules_score, ml_score, composite,
                     f"{row.created_date}T00:00:00Z" if row.created_date else None),
                )

                # Audit log entry
                cur.execute(
                    """INSERT INTO investigation_audit_log (
                        investigation_id, action_type, new_status, note
                    ) VALUES (%s, 'auto_generated', %s::investigation_status, %s)""",
                    (row.investigation_id, row.status,
                     f"Investigation auto-generated from FWA pipeline. Rules score: {rules_score:.3f}, ML score: {ml_score:.3f}."),
                )
                count += 1
            conn.commit()
    print(f"  {count} investigation cases seeded.")

    # Seed evidence for top 20 investigations
    try:
        top_inv = spark.sql(f"""
            SELECT investigation_id, target_id, target_type
            FROM {catalog_sql}.fe_bar_fwa.silver_fwa_investigation_cases
            ORDER BY rules_risk_score DESC LIMIT 20
        """).collect()
    except Exception:
        top_inv = []

    evidence_count = 0
    with get_pg_connection(LAKEBASE_PROJECT_ID, "fwa_cases") as conn:
        with conn.cursor() as cur:
            # Clear auto-seeded evidence for idempotent re-runs
            cur.execute("DELETE FROM investigation_evidence WHERE evidence_type = 'claim'")
            conn.commit()
            for inv in top_inv:
                if inv.target_type == "provider":
                    where = f"provider_npi = '{inv.target_id}'"
                elif inv.target_type == "member":
                    where = f"member_id = '{inv.target_id}'"
                else:
                    continue

                claims = spark.sql(f"""
                    SELECT signal_id, claim_id, fraud_type, fraud_score,
                           evidence_summary, estimated_overpayment
                    FROM {catalog_sql}.fe_bar_fwa.silver_fwa_signals
                    WHERE {where} LIMIT 10
                """).collect()

                for claim in claims:
                    cur.execute(
                        """INSERT INTO investigation_evidence (
                            investigation_id, evidence_type, reference_id, description, detail_json
                        ) VALUES (%s, 'claim', %s, %s, %s::jsonb)""",
                        (inv.investigation_id, claim.claim_id, claim.evidence_summary,
                         json.dumps({"signal_id": claim.signal_id, "fraud_type": claim.fraud_type,
                                     "fraud_score": float(claim.fraud_score),
                                     "estimated_overpayment": float(claim.estimated_overpayment)})),
                    )
                    evidence_count += 1
            conn.commit()
    print(f"  {evidence_count} evidence records seeded.")

    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed Operational Data from Gold Tables
# MAGIC
# MAGIC **Run this step after pipelines have completed** — it reads from gold tables to
# MAGIC populate Lakebase with FWA investigation cases and seeded evidence.

# COMMAND ----------

print("=" * 60)
print("STEP 5: Seed Operational Data from Gold Tables")
print("=" * 60)

print("\n--- FWA Investigations ---")
inv_count = seed_fwa_investigations()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restart Apps
# MAGIC Apps cache Genie space IDs and Lakebase connections at startup.
# MAGIC After bootstrap creates these resources and grants permissions,
# MAGIC apps must be restarted to pick up the new configuration.

# COMMAND ----------

print("=" * 60)
print("STEP 6: Deploy & Restart Apps")
print("=" * 60)

# Map app names to their source code directories (relative to bundle root).
# The DAB creates the app resource but does NOT deploy source code automatically.
# This step ensures source code is deployed and apps are restarted with fresh config.
APP_SOURCE_CODE_MAP = {
    "red-bricks-fwa-portal-app": "app-fwa",
}

deploy_count = 0
for app_name in APP_NAME_PATTERNS:
    try:
        app_info = w.apps.get(app_name)
        source_dir = APP_SOURCE_CODE_MAP.get(app_name)

        if source_dir:
            source_path = str(_repo_root / source_dir)
            print(f"  Deploying {app_name} from {source_path}...")
            try:
                w.apps.deploy(
                    app_name=app_name,
                    app_deployment=AppDeployment(
                        source_code_path=source_path,
                        mode=AppDeploymentMode.SNAPSHOT,
                    ),
                ).result()
                print(f"    {app_name}: deployed ✓")
                deploy_count += 1
            except Exception as deploy_err:
                print(f"    {app_name}: deploy failed ({deploy_err}), trying stop+start instead...")
                try:
                    w.apps.stop(name=app_name).result()
                    w.apps.start(name=app_name).result()
                    print(f"    {app_name}: restarted ✓")
                    deploy_count += 1
                except Exception as restart_err:
                    print(f"    {app_name}: restart also failed ({restart_err})")
        else:
            # No source code mapping — just restart
            print(f"  Restarting {app_name} (no source code mapping)...")
            w.apps.stop(name=app_name).result()
            w.apps.start(name=app_name).result()
            print(f"    {app_name}: restarted ✓")
            deploy_count += 1
    except Exception as e:
        print(f"  {app_name}: failed ({e})")

print(f"\n  {deploy_count} apps deployed/restarted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("BOOTSTRAP COMPLETE")
print("=" * 60)

# Lakebase status
try:
    project = w.postgres.get_project(name=f"projects/{LAKEBASE_PROJECT_ID}")
    print(f"\n  Lakebase project: {LAKEBASE_PROJECT_ID} (uid: {project.uid})")
except Exception as e:
    print(f"\n  Lakebase project: {LAKEBASE_PROJECT_ID} (could not check: {e})")

for cfg in LAKEBASE_DATABASES:
    print(f"    Database: {cfg['database_name']}")
    try:
        with get_pg_connection(LAKEBASE_PROJECT_ID, cfg["database_name"]) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
                table_count = cur.fetchone()[0]
                print(f"      Tables: {table_count}")
    except Exception as e:
        print(f"      Connection check: {e}")

# App SPs
print(f"\n  App service principals granted: {len(app_sps)}")
for sp in app_sps:
    print(f"    {sp['app_name']}: {sp['sp_name']}")

# UC
print(f"\n  UC catalog: {catalog}")
print(f"  Schemas with grants: {', '.join(UC_SCHEMAS)}")
print(f"  Warehouse: {warehouse_id or '(not set)'}")

# Genie spaces
try:
    _genie_list = _requests.get(
        f"https://{_host}/api/2.0/genie/spaces",
        headers=_genie_headers,
    ).json().get("spaces", [])
    print(f"\n  Genie spaces: {len(_genie_list)}")
    for s in _genie_list:
        print(f"    {s.get('title', 'untitled')} ({s['space_id']})")
except Exception:
    print("\n  Genie spaces: could not list")

# Seeded data
print(f"\n  Investigations seeded: {inv_count}")
print(f"  ML predictions table: {catalog}.fe_bar_fwa.fwa_ml_predictions")

print(f"\n  Apps deployed/restarted: {deploy_count}")

print("\n" + "=" * 60)
print("Next steps:")
print("  1. If gold tables weren't ready, re-run Steps 5-6 after pipelines complete")
print("  2. Verify apps at their URLs — all connections and permissions are live")
print("=" * 60)
