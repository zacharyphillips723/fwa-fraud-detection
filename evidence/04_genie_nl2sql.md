# Execution Evidence — Genie (natural-language querying)

Captured 2026-09-28 15:56 UTC. A real question posed to the **FWA — Fraud Detection & Investigation** Genie space (`01f1bb50a89a1e48b198665702979b0e`)
via the Genie Conversation API — showing the SQL Genie generated and the live result it returned.

### Natural-language question
> Which 5 providers have the highest composite risk score, and what is their total estimated overpayment?

### SQL Genie generated
```sql
WITH ranked_providers AS (
  SELECT
    `provider_npi`,
    `provider_name`,
    `composite_risk_score`,
    `fwa_estimated_overpayment`,
    RANK() OVER (ORDER BY `composite_risk_score` DESC) AS `risk_rank`
  FROM `testing_playground_catalog`.`fe_bar_fwa`.`gold_fwa_provider_risk`
  WHERE `composite_risk_score` IS NOT NULL
    AND `provider_npi` IS NOT NULL
    AND `provider_name` IS NOT NULL
)
SELECT
  `provider_npi`,
  `provider_name`,
  `composite_risk_score`,
  `fwa_estimated_overpayment`
FROM ranked_providers
WHERE `risk_rank` <= 5
ORDER BY `composite_risk_score` DESC, `provider_npi`
```

### Result Genie returned
| provider_npi | provider_name | composite_risk_score | fwa_estimated_overpayment |
|---|---|---|---|
| 1880179899 | Marks, Kara | 0.5333 | 12157.59 |
| 1880179899 | Marks, Kara | 0.5333 | 12157.59 |
| 1880179899 | Marks, Kara | 0.5333 | 12157.59 |
| 1378556835 | Douglas, Eric | 0.5236 | 8762.94 |
| 1378556835 | Douglas, Eric | 0.5236 | 8762.94 |
| 1378556835 | Douglas, Eric | 0.5236 | 8762.94 |

_6 row(s)._

### Genie narrative

Would you prefer to see the top 5 providers by a different risk metric or include all providers tied at the 5th rank?
