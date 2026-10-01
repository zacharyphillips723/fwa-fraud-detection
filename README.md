# FWA Fraud Detection — End-to-End on the Databricks Data Intelligence Platform

A working prototype that helps a **health insurer's Special Investigations Unit (SIU)** find and
prioritize **Fraud, Waste & Abuse (FWA)** in medical and pharmacy claims — from raw synthetic data
all the way to a business-facing investigation app. Built as a single, integrated data journey
across Lakeflow, Unity Catalog, Lakebase, ML + GenAI, Genie, and Databricks Apps.

> Extracted as a standalone, portable build from the larger `red-bricks-insurance` demo. All data is
> **synthetic** — no real claims, members, providers, or PHI/PII.

---

## For the reviewer — proof it ran, how it was built, where the code is

**It ran end to end.** The Databricks job `fwa_fraud_detection_demo` completed **16/16 tasks SUCCESS**
(run `158024769379945`, serverless, zero manual steps). Full task log + model metrics:
[`evidence/00_job_run.md`](evidence/00_job_run.md). All proof is committed as **readable text** in
[`evidence/`](evidence/) — not screenshots.

**How it was built** — AI tools, model/prompt choices, and honest trade-offs:
[**`BUILD.md`**](BUILD.md).

### Proof it ran (inline — pulled live from the run)

| Stage | Evidence | Result |
|---|---|---|
| Lakeflow | [row counts](evidence/01_pipeline_and_data.md) | 417,489 medical + 142,776 pharmacy claims; 36,015 FWA signals; 1,488 providers risk-ranked |
| ML | [model output](evidence/02_ml_model.md) + [MLflow metrics](evidence/00_job_run.md) | 139,158 claims scored; train AUC 0.703 / recall 0.690 (honest: synthetic labels → high-recall triage, see BUILD.md) |
| GenAI | [agent evaluation](evidence/03_agent_and_genie.md) | 24-case FWA classification, LLM-judged (llama-4-maverick + claude-haiku-4-5, judge claude-sonnet-4) |
| Genie | [NL→SQL transcript](evidence/04_genie_nl2sql.md) | real question → generated SQL (`RANK() OVER …`) → 6 live rows |
| App + UC | [app + governance](evidence/05_app_and_governance.md) | app RUNNING; 5 PHI/PII column masks + LOB row filter, verified via `information_schema` |

### Governance is real, not asserted (actual DDL from [`src/notebooks/setup_uc_governance.py`](src/notebooks/setup_uc_governance.py))

```sql
CREATE OR REPLACE FUNCTION {cat}.governance.mask_ssn(ssn_val STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('phi_full_access') THEN ssn_val   -- authorized: clear
    ELSE 'XXX-XX-' || substr(ssn_val, -4)                          -- everyone else: masked
  END;
-- applied with: ALTER TABLE … ALTER COLUMN ssn_last_4 SET MASK {cat}.governance.mask_ssn;
-- + a row filter on silver_enrollment scoping rows by line-of-business.
```

### Code tour — every claim in the deck maps to an inspectable file

| Claim | File |
|---|---|
| Bronze→silver→gold medallion + DQ expectations | [`src/pipelines/fwa/{bronze,silver,gold}.sql`](src/pipelines/fwa/) |
| Column masks + row filters | [`src/notebooks/setup_uc_governance.py`](src/notebooks/setup_uc_governance.py) |
| XGBoost training → MLflow → Unity Catalog | [`src/notebooks/train_fwa_model.py`](src/notebooks/train_fwa_model.py) |
| Multi-agent supervisor (Genie + policy-RAG fan-out) | [`src/agents/fwa_supervisor_agent.py`](src/agents/fwa_supervisor_agent.py) |
| Genie space definition | [`config/genie_fwa_setup.py`](config/genie_fwa_setup.py) |
| Lakebase operational schema (cases/evidence/audit) | [`src/fwa_lakebase_schema.sql`](src/fwa_lakebase_schema.sql) |
| The one-shot job DAG | [`resources/fwa_fe_bar_job.yml`](resources/fwa_fe_bar_job.yml) |

---

## The business problem

Fraud, waste, and abuse consume an estimated **3–10% of U.S. healthcare claims spend** (NHCAA/CMS).
For a plan paying **$2B in annual claims**, even 3% is **~$60M in leakage**. SIU teams are swamped
with low-value leads and investigate reactively. This build focuses them: an ML fraud scorer plus a
governed investigation portal surface the **highest-recovery providers and claims first**, with a
natural-language agent and Genie for ad-hoc questioning.

**Buyer KPIs moved:** recovered/prevented overpayment ($), SIU analyst throughput (cases/FTE),
time-to-triage, and precision of referrals (share of investigated cases that recover).

---

## The end-to-end journey (six integrated stages)

| Stage | Platform capability | In this build |
|---|---|---|
| **Ingest** | Lakeflow / Spark Declarative Pipelines | `providers → members → claims → fwa` bronze→silver→gold medallion, Auto Loader from a UC Volume, DQ expectations |
| **Govern** | Unity Catalog | Single governed schema, data-quality expectations, tags, column masks / row filters, lineage |
| **Serve** | Lakebase (managed Postgres) | Operational investigation state — case queue, assignments, evidence, immutable audit trail |
| **Make it intelligent** | ML **+** GenAI | XGBoost claim-level fraud scorer (MLflow → UC, served endpoint) **and** a multi-agent investigation assistant (tool-calling + Genie sub-agent + medical-policy RAG) |
| **Query in natural language** | Genie | Genie space over the FWA gold tables with curated sample questions |
| **Surface to the business** | Databricks App | FWA Investigation Portal (React + FastAPI): dashboard, prioritized queue, case detail, provider risk + SHAP, network graph, agent chat, Genie search, agent observability |

---

## Architecture

```
                          Synthetic generators (Faker + fpdf)
                                       │  parquet + medical-policy PDFs → UC Volume
                                       ▼
   Lakeflow SDP medallion   providers ─┐
   (bronze→silver→gold)     members  ──┼─► fwa_pipeline ─► gold analytics MVs
                            claims   ──┘        │
                                                ▼
                     XGBoost fraud scorer (MLflow → Unity Catalog @champion)
                        │ writes fwa_ml_predictions + fwa_model_inference
                        ▼
   Multi-agent supervisor ── Genie sub-agent (NL→SQL) ── medical-policy RAG (Vector Search)
                        │
                        ▼
   Lakebase (fwa_cases): investigations · assignments · evidence · audit
                        │
                        ▼
   FWA Investigation Portal (Databricks App)  +  Genie space  +  agent OTel traces in UC
```

Everything lands in a **single Unity Catalog schema** (`${catalog}.${schema}`, default
`testing_playground_catalog.fe_bar_fwa`) — tables, the raw volume, the registered model, the Vector
Search index, and the agent trace tables.

---

## Deploy

**Prerequisites:** a Databricks workspace with Unity Catalog, serverless SQL + serverless jobs,
Foundation Model APIs (`databricks-llama-4-maverick`, `databricks-claude-haiku-4-5`), Vector Search,
and Lakebase enabled. Databricks CLI ≥ 0.220 authenticated with a profile.

```bash
# 1. Point at your workspace (default target is fevm-testing-playground)
databricks bundle validate -t fe-bar

# 2. Deploy pipelines, job, dashboards, and the app
databricks bundle deploy -t fe-bar

# 3. Run the full build end-to-end (raw data → app)
databricks bundle run fwa_fraud_detection_demo -t fe-bar
```

The single job orchestrates the whole journey: `setup_lakebase` + `data_generation` → medallion
pipelines → `train_fwa_model` → gold analytics → governance → medical-policy VS + Genie +
multi-agent deploy → evaluation + model monitoring → `bootstrap_workspace` (grants + Lakebase seed
+ trace tables) → `deploy_app_source`.

To retarget another workspace, add a target in `databricks.yml` with your profile and (if you can
create catalogs) override `catalog`; otherwise keep the single-schema model and set `schema`.

---

## Execution evidence

`evidence/` contains committed, **text-readable** proof the build ran end-to-end — captured live
from a **fully-green job run (16/16 tasks SUCCESS)** on serverless (run `158024769379945`):

- **[01_pipeline_and_data.md](evidence/01_pipeline_and_data.md)** — medallion row counts (417K medical claims, 143K pharmacy, 36K FWA signals, 1,488 providers scored) + governed gold samples
- **[02_ml_model.md](evidence/02_ml_model.md)** — XGBoost fraud scorer: risk-tier distribution, top flagged claims, 139K scored
- **[03_agent_and_genie.md](evidence/03_agent_and_genie.md)** — 24-case multi-agent evaluation, LLM-judged (llama-4-maverick + claude-haiku-4-5, judge claude-sonnet-4)
- **[04_genie_nl2sql.md](evidence/04_genie_nl2sql.md)** — real NL question → Genie-generated SQL → live result
- **[05_app_and_governance.md](evidence/05_app_and_governance.md)** — app running, Lakebase-backed, 5 PHI/PII column masks + row filter

---

## Repository layout

```
fwa-fraud-detection/
├── databricks.yml              # DAB: catalog/schema vars, fe-bar target
├── resources/                  # pipelines, the full-build job, app, dashboard, AI gateway, Lakebase
├── src/
│   ├── data_generation/        # Faker/fpdf synthetic generators (providers, members, claims, fwa, policies)
│   ├── pipelines/              # SDP SQL: providers, members, claims, fwa, gold_analytics/fwa_analytics
│   ├── agents/                 # fwa_investigation_agent.py, fwa_supervisor_agent.py
│   ├── notebooks/              # train, deploy, evaluate, governance, monitoring, bootstrap, Genie/VS setup
│   ├── fwa_lakebase_schema.sql # operational Postgres schema (investigations/evidence/audit)
│   └── dashboards/             # model-monitoring + agent-observability Lakeview dashboards
├── app-fwa/                    # FWA Investigation Portal (React + FastAPI, Lakebase-backed)
├── lib/shared_backend/         # canonical Lakebase connection modules
└── config/                     # Lakebase + Genie space setup
```

## Key packages

`databricks-sdk`, `mlflow`, `xgboost`, `scikit-learn`, `pyspark`, `databricks-vectorsearch`,
`faker`, `fpdf2`, `PyMuPDF`; app: `fastapi`, `uvicorn`, `sqlalchemy`, React + Vite + TypeScript.

---

*Synthetic data only. Built on the Databricks Data Intelligence Platform.*
