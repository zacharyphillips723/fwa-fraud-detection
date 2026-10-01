# Execution Evidence — the job actually ran end to end

This is the proof the whole journey executed, not just that the code exists. Everything below
is committed text pulled from the live Databricks job run — no screenshots.

## The run

- **Job:** `fwa_fraud_detection_demo` (Databricks Workflow, serverless)
- **Run:** `158024769379945` on `fevm-testing-playground`
- **Result:** `SUCCESS` — **16/16 tasks green, zero manual intervention**
- Run page: `https://fevm-testing-playground.cloud.databricks.com/#job/143077489290671/run/158024769379945`

## Task-level log (result + wall-clock duration)

| Task | Result | Duration | Stage |
|---|---|---|---|
| setup_lakebase | SUCCESS | 49s | Lakebase provisioning |
| data_generation | SUCCESS | 560s | Synthetic claims + policy PDFs |
| providers_pipeline | SUCCESS | 63s | Lakeflow (SDP) |
| members_pipeline | SUCCESS | 63s | Lakeflow (SDP) |
| claims_pipeline | SUCCESS | 82s | Lakeflow (SDP) |
| fwa_pipeline | SUCCESS | 63s | Lakeflow (SDP) — bronze→silver→gold |
| train_fwa_model | SUCCESS | 259s | ML — XGBoost + MLflow → Unity Catalog |
| gold_analytics_pipeline | SUCCESS | 62s | Lakeflow (SDP) — gold analytics MVs |
| setup_uc_governance | SUCCESS | 67s | Unity Catalog — masks + row filters |
| setup_ai_gateway | SUCCESS | 26s | Mosaic AI Gateway |
| setup_medical_policy_vs | SUCCESS | 45s | Vector Search — policy RAG index |
| genie_fwa_setup | SUCCESS | 37s | Genie space |
| deploy_fwa_supervisor_agent | SUCCESS | 73s | GenAI — multi-agent serving endpoint |
| evaluate_fwa_agent | SUCCESS | 198s | GenAI — agent evaluation |
| bootstrap_workspace | SUCCESS | 68s | Grants + Lakebase seed + OTel trace tables |
| deploy_app_source | SUCCESS | 171s | Databricks App deploy |

## Model training output (MLflow)

Real metrics logged by `train_fwa_model` to MLflow and registered to Unity Catalog as
`testing_playground_catalog.fe_bar_fwa.fwa_scoring_model`:

| Metric | Value |
|---|---|
| train_auc_roc | 0.703 |
| train_recall | 0.690 |
| train_f1 | 0.111 |
| train_precision | 0.060 |
| best_cv_auc | 0.532 |

**Honest read:** these are deliberately *not* dressed up. The fraud labels are heuristic/synthetic,
so the scorer behaves like a high-recall **triage prioritizer** (it surfaces most suspicious claims,
with low precision) rather than a production classifier. In this build the ML score is **one signal
in a layered system** — rules-based FWA signals, the XGBoost score, the multi-agent investigator, and
medical-policy RAG — and it is the layering, plus the SHAP feature-contribution explanation shown to
the investigator, that carries the value. On real labeled recovery outcomes this model would sharpen
materially; see `BUILD.md` for the full trade-off discussion.
