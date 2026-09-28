# Execution Evidence — FWA Fraud Detection

Captured 2026-09-28 15:54 UTC from a fully-green job run (16/16 tasks SUCCESS) on Databricks serverless.
Catalog `testing_playground_catalog`, schema `fe_bar_fwa`. All queries below were executed live against the tables the pipeline produced.

## 1. Lakeflow pipeline output — row counts & governed gold samples

### Row counts across the medallion, ML, and RAG layers

```sql
SELECT 'silver_fwa_signals' AS tbl, count(*) AS rows FROM silver_fwa_signals
UNION ALL SELECT 'silver_claims_medical', count(*) FROM silver_claims_medical
UNION ALL SELECT 'silver_claims_pharmacy', count(*) FROM silver_claims_pharmacy
UNION ALL SELECT 'silver_members', count(*) FROM silver_members
UNION ALL SELECT 'silver_providers', count(*) FROM silver_providers
UNION ALL SELECT 'gold_fwa_provider_risk', count(*) FROM gold_fwa_provider_risk
UNION ALL SELECT 'gold_fwa_claim_flags', count(*) FROM gold_fwa_claim_flags
UNION ALL SELECT 'gold_fwa_model_scores', count(*) FROM gold_fwa_model_scores
UNION ALL SELECT 'fwa_ml_predictions', count(*) FROM fwa_ml_predictions
UNION ALL SELECT 'medical_policy_chunks', count(*) FROM medical_policy_chunks
ORDER BY rows DESC
```

| tbl | rows |
|---|---|
| gold_fwa_model_scores | 1431009 |
| silver_claims_medical | 417489 |
| gold_fwa_claim_flags | 286587 |
| silver_claims_pharmacy | 142776 |
| fwa_ml_predictions | 139158 |
| silver_fwa_signals | 36015 |
| silver_members | 15000 |
| silver_providers | 1488 |
| gold_fwa_provider_risk | 1488 |
| medical_policy_chunks | 500 |

_10 row(s)._

### Aggregate FWA summary (gold_fwa_summary)

```sql
SELECT * FROM gold_fwa_summary ORDER BY 1 LIMIT 25
```

| fraud_type | fraud_type_desc | severity | risk_bucket | detection_method | line_of_business | service_year_month | signal_count | distinct_claims | distinct_providers | distinct_members | total_paid_amount | total_estimated_overpayment | avg_fraud_score | max_fraud_score | overpayment_ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | geographic_analysis | Medicare Advantage | 2025-04-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 0.0 | 0.0 | 0.18289999999999998 | 0.1829 |  |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | High | Critical | peer_comparison | ACA Marketplace | 2024-09-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 1431.8999999999999 | 528.48 | 0.8335 | 0.8335 | 0.3691 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | rules_engine | ACA Marketplace | 2025-04-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 37.53 | 19.89 | 0.1532 | 0.1532 | 0.53 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Critical | Critical | ai_model_v1 | Medicaid | 2023-10-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 11677.14 | 8085.960000000001 | 0.8321999999999999 | 0.8322 | 0.6925 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | temporal_pattern | ACA Marketplace | 2024-07-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 5934.150000000001 | 3861.09 | 0.2947 | 0.2947 | 0.6507 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | High | High | network_analysis | Commercial | 2024-01-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 2553.93 | 931.6800000000001 | 0.7557 | 0.7557 | 0.3648 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Medium | Low | temporal_pattern | Commercial | 2025-08-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 378.45 | 201.78000000000003 | 0.38449999999999995 | 0.3845 | 0.5332 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Medium | Low | ai_model_v1 | ACA Marketplace | 2024-04-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 25007.22 | 13246.380000000001 | 0.3917 | 0.3917 | 0.5297 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Medium | Medium | peer_comparison | ACA Marketplace | 2024-02-01T00:00:00.000Z | 27 | 3 | 3 | 3 | 13568.940000000002 | 5320.4400000000005 | 0.5198333333333333 | 0.5582 | 0.3921 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | ai_model_v1 | ACA Marketplace | 2025-07-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 0.0 | 0.0 | 0.2689 | 0.2689 |  |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Medium | Medium | statistical_outlier | Medicare Advantage | 2025-02-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 379.98 | 156.78000000000003 | 0.579 | 0.579 | 0.4126 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | High | Medium | peer_comparison | Commercial | 2024-10-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 77.94 | 48.87 | 0.599 | 0.599 | 0.627 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Critical | Critical | ai_model_v1 | Medicare Advantage | 2023-12-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 0.0 | 0.0 | 0.8314 | 0.8314 |  |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | High | High | ai_model_v1 | Medicare Advantage | 2023-12-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 319.95 | 158.03999999999996 | 0.6727 | 0.6727 | 0.494 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Medium | Medium | statistical_outlier | ACA Marketplace | 2024-03-01T00:00:00.000Z | 18 | 2 | 2 | 2 | 37847.88 | 20221.11 | 0.4835499999999999 | 0.5408 | 0.5343 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Medium | Low | rules_engine | Commercial | 2024-05-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 13827.600000000002 | 5700.150000000001 | 0.389 | 0.389 | 0.4122 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | rules_engine | Medicare Advantage | 2023-11-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 0.0 | 0.0 | 0.33960000000000007 | 0.3396 |  |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | geographic_analysis | ACA Marketplace | 2023-06-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 558.54 | 320.04 | 0.1379 | 0.1379 | 0.573 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | ai_model_v1 | Commercial | 2023-11-01T00:00:00.000Z | 18 | 2 | 2 | 2 | 5662.4400000000005 | 2453.4900000000002 | 0.2575 | 0.3973 | 0.4333 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Medium | Medium | rules_engine | ACA Marketplace | 2024-07-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 28713.96 | 11222.550000000001 | 0.55 | 0.55 | 0.3908 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | High | High | network_analysis | Commercial | 2024-04-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 8694.0 | 3236.4000000000005 | 0.6679 | 0.6679 | 0.3723 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Medium | Medium | statistical_outlier | Medicare Advantage | 2024-06-01T00:00:00.000Z | 18 | 2 | 2 | 2 | 827.5500000000001 | 350.37 | 0.4723 | 0.4882 | 0.4234 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | ai_model_v1 | Commercial | 2025-06-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 24903.899999999998 | 8558.279999999999 | 0.24860000000000002 | 0.2486 | 0.3437 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | peer_comparison | Medicare Advantage | 2024-08-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 4522.59 | 2251.44 | 0.2063 | 0.2063 | 0.4978 |
| doctor_shopping | Doctor Shopping — Member seeing 5+ prov… | Low | Low | rules_engine | Commercial | 2099-01-01T00:00:00.000Z | 9 | 1 | 1 | 1 | 514.8000000000001 | 294.3 | 0.3094 | 0.3094 | 0.5717 |

_25 row(s)._

### Top 10 highest-risk providers (gold_fwa_provider_risk)

```sql
SELECT provider_npi, provider_name, specialty, signal_count, total_estimated_overpayment, composite_risk_score, rank_by_risk FROM gold_fwa_provider_risk ORDER BY composite_risk_score DESC LIMIT 10
```

ERROR: {"error_code": "BAD_REQUEST", "message": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `signal_count` cannot be resolved. Did you mean one of the following? [`fwa_signal_count`, `denial_rate`, `total_paid`, `total_claims`, `fwa_avg_score`]. SQLSTATE: 42703;

### FWA signals by fraud type

```sql
SELECT fraud_type, count(*) AS signals, round(sum(estimated_overpayment),0) AS est_overpayment FROM silver_fwa_signals GROUP BY fraud_type ORDER BY signals DESC
```

| fraud_type | signals | est_overpayment |
|---|---|---|
| upcoding | 6237 | 1185577.0 |
| duplicate_billing | 6066 | 2740696.0 |
| short_refill | 5454 | 1177438.0 |
| unbundling | 4695 | 705588.0 |
| drug_switching | 4245 | 795807.0 |
| doctor_shopping | 3045 | 675861.0 |
| impossible_day | 2457 | 791034.0 |
| phantom_billing | 2190 | 1010883.0 |
| provider_ring | 1626 | 150981.0 |

_9 row(s)._

