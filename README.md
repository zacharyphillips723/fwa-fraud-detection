# FWA Fraud Detection — End-to-End on the Databricks Data Intelligence Platform

A working prototype that helps a **health insurer's Special Investigations Unit (SIU)** find and
prioritize **Fraud, Waste & Abuse (FWA)** in medical and pharmacy claims — from raw synthetic data
all the way to a business-facing investigation app. Built as a single, integrated data journey
across Lakeflow, Unity Catalog, Lakebase, ML + GenAI, Genie, and Databricks Apps.

> Extracted as a standalone, portable build from the larger `red-bricks-insurance` demo. All data is
> **synthetic** — no real claims, members, providers, or PHI/PII.

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

`evidence/` contains committed, **text-readable** proof the build ran: pipeline row counts and
sample rows, MLflow model metrics (AUC / PR / confusion matrix), a real multi-agent investigation
trace (question → tool calls → answer), Genie NL→SQL→results, and app API responses. _(Populated
after the first full run — see the FE Bar submission checklist.)_

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
