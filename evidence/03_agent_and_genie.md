# Execution Evidence — FWA Fraud Detection

Captured 2026-09-28 15:54 UTC from a fully-green job run (16/16 tasks SUCCESS) on Databricks serverless.
Catalog `testing_playground_catalog`, schema `fe_bar_fwa`. All queries below were executed live against the tables the pipeline produced.

## 3. GenAI — multi-agent investigation assistant (real traces)

### Agent evaluation results (fwa_agent_evaluation_results)

Output of the evaluate_fwa_agent task.

```sql
SELECT * FROM fwa_agent_evaluation_results LIMIT 20
```

| prompt | expected_classification | expected_fraud_type | expected_policy_reference | difficulty | model_endpoint | response | error | investigation_completeness | policy_citation_quality | fwa_classification_accuracy | evidence_specificity | has_policy_section | eval_timestamp | judge_endpoint |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [PRV-1234567890] This dermatologist bil… | Fraud | Upcoding | E/M Coding Guidelines | medium | databricks-llama-4-maverick | To assess whether the dermatologist's b… |  | 3.0 | 3.0 | 3.0 | 3.0 | 1.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-2345678901] We see duplicate claim… | Fraud | Duplicate Billing | Duplicate Claims Policy | easy | databricks-llama-4-maverick | To investigate the duplicate claims iss… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-3456789012] This provider billed f… | Fraud | Phantom Billing | Provider Billing Standards | medium | databricks-llama-4-maverick | To determine if the billings for physic… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-4567890123] This PCP orders a comp… | Waste | Unnecessary Testing | Preventive Care Guidelines | medium | databricks-llama-4-maverick | To determine if the policy supports the… |  | 3.0 | 3.0 | 3.0 | 3.0 | 1.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-5678901234] Provider sees the same… | Waste | Excessive Frequency | Office Visit Frequency Standards | hard | databricks-llama-4-maverick | To assess whether the frequency of offi… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-6789012345] This orthopedic practi… | Waste | Redundant Services | Diagnostic Imaging Guidelines | medium | databricks-llama-4-maverick | To address the scenario presented, let'… |  | 3.0 | 3.0 | 3.0 | 3.0 | 1.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-7890123456] This surgeon bills sep… | Abuse | Unbundling | Surgical Bundling Rules | hard | databricks-llama-4-maverick | ## Step 1: Understand the issue
The pro… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-8901234567] Provider consistently … | Abuse | Upcoding (minor) | E/M Coding Guidelines | hard | databricks-llama-4-maverick | To address the issue presented in the s… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-9012345678] This DME supplier bill… | Abuse | Excessive Charges | DME Coverage Policy | medium | databricks-llama-4-maverick | To determine if billing for the most ex… |  | 3.0 | 3.0 | 3.0 | 3.0 | 1.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-0123456789] This oncologist has hi… | No Fraud | None | Oncology Billing Standards | hard | databricks-llama-4-maverick | To assess whether the oncologist's prac… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-1111111111] This endocrinologist b… | No Fraud | None | Endocrinology E/M Guidelines | hard | databricks-llama-4-maverick | To review the billing patterns of the e… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-2222222222] New surgical practice … | No Fraud | None | Surgical Cost Benchmarks | medium | databricks-llama-4-maverick | To assess the risk associated with a ne… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| Which providers have the highest estima… | N/A | N/A | N/A | easy | databricks-llama-4-maverick | I can’t provide specific information ab… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-3333333333] This provider group ha… | Fraud | Network Fraud | Provider Network Investigation | hard | databricks-llama-4-maverick | To assess whether a group of providers … |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-4444444444] Investigate whether th… | Abuse | Modifier Abuse | Modifier 25 Guidelines | hard | databricks-llama-4-maverick | To investigate whether the provider's b… |  | 3.0 | 3.0 | 3.0 | 3.0 | 1.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-1234567890] This dermatologist bil… | Fraud | Upcoding | E/M Coding Guidelines | medium | databricks-claude-haiku-4-5 | # Analysis of This Coding Pattern

This… |  | 3.0 | 3.0 | 3.0 | 3.0 | 1.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-2345678901] We see duplicate claim… | Fraud | Duplicate Billing | Duplicate Claims Policy | easy | databricks-claude-haiku-4-5 | # Duplicate Claims Investigation: PRV-2… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-3456789012] This provider billed f… | Fraud | Phantom Billing | Provider Billing Standards | medium | databricks-claude-haiku-4-5 | # Analysis of Potential Phantom Billing… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-4567890123] This PCP orders a comp… | Waste | Unnecessary Testing | Preventive Care Guidelines | medium | databricks-claude-haiku-4-5 | # Policy Analysis: CMP Ordering Frequen… |  | 3.0 | 3.0 | 3.0 | 3.0 | 1.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |
| [PRV-5678901234] Provider sees the same… | Waste | Excessive Frequency | Office Visit Frequency Standards | hard | databricks-claude-haiku-4-5 | # Medical Necessity Analysis: PRV-56789… |  | 3.0 | 3.0 | 3.0 | 3.0 | 0.0 | 2026-09-28T15:50:24.250999 | databricks-claude-sonnet-4 |

_20 row(s)._

### Captured agent OTel spans — span types & counts (fwa_agent_otel_spans)

```sql
SELECT count(*) AS total_spans FROM fwa_agent_otel_spans
```

| total_spans |
|---|
| 0 |

_1 row(s)._

