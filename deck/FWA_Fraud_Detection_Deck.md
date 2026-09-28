# FWA Fraud Detection
### Turning claims data into recovered dollars

A working prototype for a health insurer's Special Investigations Unit — an end-to-end data journey on the Databricks Data Intelligence Platform. *(Synthetic data only.)*

---

## Fraud, waste & abuse is a silent margin leak

- FWA runs an estimated **3–10% of claims spend** (NHCAA / CMS).
- For a plan paying **$2B in annual claims**, that is **$60M–$200M leaking every year**.
- SIU teams triage **reactively**, buried in low-value leads and manual review.
- Every week a case waits, overpayment becomes **unrecoverable**.

**The question isn't whether FWA is happening — it's which providers and claims to work first.**

---

## The outcome we drive — in your KPIs

| KPI | What moves |
|---|---|
| **Recovered / prevented overpayment ($)** | The headline number — dollars back on existing spend, quarter over quarter |
| **SIU productivity (cases / FTE)** | More cases closed per investigator; triage in minutes, not weeks |
| **Time-to-triage** | Highest-risk providers surface automatically, ranked by recovery potential |
| **Referral precision** | Higher share of investigated providers that actually recover |

---

## One integrated data journey — not six tools

| Stage | Capability |
|---|---|
| **Ingest** | **Lakeflow** — raw claims into a governed bronze→silver→gold medallion |
| **Govern** | **Unity Catalog** — PHI/PII masking, row filters, lineage, tags |
| **Serve** | **Lakebase** — operational case management (queue, evidence, audit) |
| **Make it intelligent** | **ML + GenAI** — XGBoost fraud scorer *and* a multi-agent investigator |
| **Query in natural language** | **Genie** — ask the data in plain English |
| **Surface to the business** | **Databricks App** — the FWA Investigation Portal |

One platform, one governance model, one copy of the data.

---

## Architecture: raw data to recovered dollars

```
Synthetic claims (Faker + policy PDFs)
        │
   Lakeflow SDP medallion   providers ┐
   (bronze→silver→gold)     members  ─┼─► fwa_pipeline ─► gold analytics
                            claims   ─┘        │
                                               ▼
        XGBoost fraud scorer (MLflow → Unity Catalog @champion)
                                               │
   Multi-agent supervisor ─ Genie sub-agent ─ medical-policy RAG (Vector Search)
                                               │
   Lakebase (fwa_cases): investigations · evidence · audit
                                               │
        FWA Investigation Portal (Databricks App)  +  Genie  +  UC audit trail
```

Everything lands in **one governed Unity Catalog schema** — tables, the model, the RAG index, and the agent trace tables.

---

## What we built — and proof it ran

Captured live from a fully-green end-to-end run (16/16 tasks) on serverless:

| Signal | Result |
|---|---|
| Medical claims ingested | **417,489** |
| Pharmacy claims ingested | **142,776** |
| FWA signals detected | **36,015** |
| Providers risk-ranked | **1,488** |
| Claims scored by the model | **139,158** |
| Multi-agent evaluation cases (LLM-judged) | **24** |
| PHI/PII protections enforced | **5 column masks + line-of-business row filter** |

*Execution evidence — row counts, model output, agent evaluation, Genie SQL — is committed as text in the repo's `evidence/` folder.*

---

## Value for the executive sponsor
### CFO / SIU Director

- **Direct financial recovery** on spend you are already making — not a cost center.
- **Governed and auditable end to end** — PHI/PII masks, full lineage, and captured agent traces stand up to compliance and audit.
- **No new infrastructure** — it runs on the Databricks platform you already govern; nothing new to procure or secure.

---

## Value for the domain owner
### SIU Investigator

- **A prioritized, risk-ranked queue** — start each day on the highest-recovery leads, not a spreadsheet.
- **Investigate in plain English** — the agent and Genie pull claims, provider history, ML scores, and medical policy on demand.
- **Evidence and audit trail captured automatically** — every action and rationale is logged for the case file.

---

## Why Databricks

- **One platform** — ingestion, governance, ML, GenAI, and the app on a single lakehouse. No stitching, no data copies.
- **Governance built in** — PHI/PII masking, lineage, and audit enforced at the platform layer, not bolted on in app code.
- **AI where the data lives** — the fraud model, the agents, and Genie all read the same governed tables.

---

## Next steps

- Point the pipeline at a **de-identified extract** of your claims.
- **Calibrate** the fraud model and rules to your denial and billing patterns.
- **Pilot** with your SIU team on a live case backlog.
- **Measure** recovered $ and time-to-triage against today's baseline.

**Let's run a scoped pilot on your data.**
