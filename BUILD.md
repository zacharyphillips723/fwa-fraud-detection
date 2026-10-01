# How this was built — AI tools, choices, and trade-offs

This is the honest account of how the FWA Fraud Detection build was made: the AI tooling I leaned
on, the model and prompt decisions behind the multi-agent design, and the trade-offs I actually hit.
Execution proof lives in [`evidence/`](evidence/) (start with [`evidence/00_job_run.md`](evidence/00_job_run.md)).

## How I built it

I did not start from a blank repo. I had an existing, larger multi-domain insurance demo and used
**Claude Code (Opus)** as the build agent to **extract the FWA vertical slice into a clean,
standalone Asset Bundle** and get it running end to end on a Databricks FE vending-machine workspace.
The work split into: (1) carve out only the FWA-relevant pipelines, agents, app, and setup;
(2) re-home everything into a single Unity Catalog schema; (3) wire a one-shot job DAG; (4) deploy,
run, and debug on serverless until green; (5) capture evidence.

## AI tools used, and where they were a force multiplier

- **Claude Code (Opus 4.8)** — the primary builder. The highest-leverage use was the **schema
  refactor and the serverless debugging loop**. Collapsing the slice into one schema touched **221
  cross-schema references across 21 files** (SQL `FROM schema.table`, Python `*_SCHEMA` constants,
  agent allowed-schema lists, the app env, Genie and Lakebase config). Doing that by hand is error-prone
  grind; the agent did it with boundary-aware rewrites and caught its own mistakes (BSD `sed` has no
  `\b`; `fwa.` is a substring of `fe_bar_fwa.` so a naive re-run double-prefixed — both caught and fixed).
  It then ran the job, read the task-level failures, and fixed them in a tight loop (see trade-offs below).
- **Databricks FE skills + CLIs** — FEVM for the workspace, the Statement Execution API and Genie
  Conversation API to capture evidence, the DAB CLI to deploy/run.
- **Runtime AI in the solution itself** — `databricks-llama-4-maverick` orchestrates the supervisor
  agent; `databricks-claude-haiku-4-5` is the clinical/policy sub-agent over medical-policy RAG;
  **Genie** is the structured-data sub-agent (governed NL→SQL); `databricks-claude-sonnet-4` is the
  LLM judge in the agent evaluation.

## Model & prompt choices behind the multi-agent design

- **Why a supervisor fan-out instead of one big agent.** An FWA investigator's question has two very
  different shapes: "what do the numbers say about this provider" (structured claims analytics) and
  "does this pattern violate policy" (unstructured medical-policy reasoning). I routed the first to a
  **Genie sub-agent** so the SQL is governed, inspectable, and runs against the same gold tables as
  the dashboard — not free-form model-written SQL — and the second to a **tool-calling sub-agent with
  Vector Search RAG** over the policy corpus. The supervisor composes both.
- **The latency trade-off I made on purpose.** The two legs have very different response times — the
  policy-RAG leg returns fast, the Genie leg has to plan and run SQL. Rather than block the whole
  answer on the slow leg, the app **streams each sub-agent's result the moment it lands** (separate
  `genie` and `gemini` SSE events), so the investigator reads the policy analysis while the claims
  query is still running. The wait feels useful instead of idle.
- **Explainability over a bare score.** The Provider Analysis view renders a **signed SHAP
  feature-contribution chart** next to the risk tier, so the investigator gets *why*, not just a number.

## Trade-offs I actually hit

1. **Dedicated catalog vs. single schema.** The clean isolation would have been a dedicated catalog
   (`fe_bar_fwa.fwa`, `.claims`, …) — a one-line change. But **FEVM workspaces don't grant
   catalog-create**, so I consolidated *everything* (tables, the raw volume, the model, the VS index,
   the agent trace tables) into one schema `testing_playground_catalog.fe_bar_fwa`. The cost was the
   221-reference rewrite above; the benefit is it runs in any FE sandbox with no elevated grants.
2. **Lakebase vs. Delta writeback.** The source demo had a branch that moved the apps *off* Lakebase
   onto Delta `app_state` tables. I deliberately forked from `main` to **keep Lakebase** — operational
   case management (queue, assignments, evidence, immutable audit) is a genuine OLTP workload, and the
   journey calls for a real operational serving layer, not an analytical table standing in for one.
3. **Serverless library gaps + Spark Connect.** `train_fwa_model` assumed `xgboost`, `shap`, and
   `databricks-feature-engineering` were present — serverless has none of them, so the first run failed;
   fix was an explicit `%pip` cell. Separately, the policy-VS setup had a `try/except` around a table
   that no longer exists in this slice, but **Spark Connect is lazy** so the missing-table error escaped
   the `try` and surfaced at the write — fixed with an explicit `spark.catalog.tableExists()` guard.
4. **Dropped the model-monitoring branch.** The drift-monitoring notebooks need a Model Serving
   endpoint created by hand plus ~1 hour of inference-table materialization — incompatible with a
   zero-intervention run. I removed them from the DAG and left them in-repo as documented optional
   post-steps rather than fake a green run around a manual step.
5. **Honest ML.** On synthetic data with heuristic fraud labels, the XGBoost scorer lands at ~0.70
   train AUC / 0.53 CV AUC with high recall and low precision (see `evidence/00_job_run.md`). I chose
   **not to dress that up**. The scorer is a high-recall **triage prioritizer**, one signal in a layered
   system (rules + ML + agent + policy RAG) with SHAP for transparency. On real labeled recovery data it
   sharpens materially — but I'd rather ship an honest triage layer than a vanity number.
6. **One job, reordered.** I merged orchestration into a single serverless DAG and **moved
   `train_fwa_model` ahead of the gold-analytics pipeline**, because the gold `gold_fwa_model_scores`
   view reads the model's prediction tables — running analytics first produced an empty-table race.

## Honest limitations

- All data is **synthetic** (Faker + generated policy PDFs); no real PHI/PII.
- Model precision is low on synthetic labels (above) — real value needs real labeled outcomes.
- `gold_fwa_network_analysis` (provider-ring detection) returned **0 rows** on this synthetic draw —
  no provider pair cleared the 20%-overlap threshold. The MV is correct; the synthetic data just
  didn't contain a ring that large.
- Model-drift monitoring is a manual post-step, not part of the automated run (trade-off #4).
