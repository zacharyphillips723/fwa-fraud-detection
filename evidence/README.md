# Execution Evidence

Text-readable proof this build ran end-to-end. Everything here was captured live from a
**fully-green job run (16/16 tasks SUCCESS)** on Databricks serverless — job
`fwa_fraud_detection_demo`, run `158024769379945`, on `fevm-testing-playground`,
catalog `testing_playground_catalog`, schema `fe_bar_fwa`.

| File | Stage | What it proves |
|---|---|---|
| [01_pipeline_and_data.md](01_pipeline_and_data.md) | Lakeflow + Unity Catalog | Medallion row counts (417K medical claims, 36K FWA signals) + governed gold samples |
| [02_ml_model.md](02_ml_model.md) | ML | XGBoost fraud scorer — risk-tier distribution, top flagged claims, coverage |
| [03_agent_and_genie.md](03_agent_and_genie.md) | GenAI | Multi-agent investigation assistant — 24-case evaluation, LLM-judged |
| [04_genie_nl2sql.md](04_genie_nl2sql.md) | Genie | Real NL question → Genie-generated SQL → live result |
| [05_app_and_governance.md](05_app_and_governance.md) | App + Lakebase + UC | App running, Lakebase-backed, PHI/PII masks + row filter |
