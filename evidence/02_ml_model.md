# Execution Evidence — FWA Fraud Detection

Captured 2026-09-28 15:55 UTC from a fully-green job run (16/16 tasks SUCCESS) on Databricks serverless.
Catalog `testing_playground_catalog`, schema `fe_bar_fwa`. Queries executed live.

## 2. ML — XGBoost claim-level fraud scorer

### Risk-tier distribution across all model-scored claims (fwa_ml_predictions)

```sql
SELECT ml_risk_tier, count(*) AS claims, round(avg(ml_fraud_probability),4) AS avg_prob, round(min(ml_fraud_probability),4) AS min_p, round(max(ml_fraud_probability),4) AS max_p FROM fwa_ml_predictions GROUP BY ml_risk_tier ORDER BY avg_prob DESC
```

| ml_risk_tier | claims | avg_prob | min_p | max_p |
|---|---|---|---|---|
| High | 22 | 0.711 | 0.7002 | 0.7342 |
| Medium | 117928 | 0.4988 | 0.4 | 0.6998 |
| Low | 21208 | 0.3386 | 0.0469 | 0.4 |

_3 row(s)._

### Highest-probability distinct claims flagged by the model

```sql
SELECT DISTINCT claim_id, provider_npi, claim_type, billed_amount, line_of_business, round(ml_fraud_probability,4) AS fraud_prob, ml_risk_tier FROM gold_fwa_model_scores ORDER BY fraud_prob DESC LIMIT 10
```

| claim_id | provider_npi | claim_type | billed_amount | line_of_business | fraud_prob | ml_risk_tier |
|---|---|---|---|---|---|---|
| MC832405556 | 1340297471 | Institutional IP | 10371.93 | Medicare Advantage | 0.7342 | High |
| MC494169517 | 1859867290 | ER | 1856.85 | ACA Marketplace | 0.7222 | High |
| MC625088961 | 1340297471 | Institutional OP | 1340.34 | Medicare Advantage | 0.7215 | High |
| MC534607308 | 1736873048 | Institutional OP | 900.62 | Medicare Advantage | 0.7205 | High |
| MC317269920 | 1694022242 | Institutional IP | 11763.47 | Medicare Advantage | 0.7203 | High |
| MC810237371 | 1772710898 | Institutional IP | 3589.45 | Commercial | 0.7201 | High |
| MC334459896 | 1736873048 | Professional |  | Medicare Advantage | 0.7179 | High |
| MC536226789 | 1655921993 | Professional | 133.75 | Medicare Advantage | 0.7159 | High |
| MC442392109 | 1773469560 | Professional | 131.48 | Medicare Advantage | 0.7092 | High |
| MC742657003 | 1772710898 | ER | 251.49 | Medicaid | 0.7092 | High |

_10 row(s)._

### Model coverage: scored claims vs. total claims

```sql
SELECT (SELECT count(*) FROM fwa_ml_predictions) AS scored_claims, (SELECT count(*) FROM fwa_training_features) AS training_rows, (SELECT count(DISTINCT claim_id) FROM silver_claims_medical) AS distinct_medical_claims
```

| scored_claims | training_rows | distinct_medical_claims |
|---|---|---|
| 139158 | 139158 | 139158 |

_1 row(s)._

