# Execution Evidence — Databricks App & Unity Catalog governance

Captured 2026-09-28 15:57 UTC.

## Databricks App — FWA Investigation Portal (deployed & running)

Live status from `databricks apps get fwa-fraud-detection-portal`:

| field | value |
|---|---|
| name | fwa-fraud-detection-portal |
| url | https://fwa-fraud-detection-portal-7474656897770431.aws.databricksapps.com |
| compute_status | ACTIVE — "App compute is running." |
| app_status | RUNNING — "App is running" |
| active_deployment | SUCCEEDED — "App started successfully" |
| deployment_id | 01f1bb544f2314d1894959db845e5750 |

The React + FastAPI portal is backed by **Lakebase** (`fwa_cases` database) for operational
investigation state — case queue, assignments, evidence, and an immutable audit trail
(schema in `src/fwa_lakebase_schema.sql`, seeded by `bootstrap_workspace` from the 225
synthetic investigation cases in `silver_fwa_investigation_cases`).

## Unity Catalog governance — PHI/PII protection (live)

`setup_uc_governance` applied real column masks and a row filter, verified via
`information_schema`:

### Column masks (PHI/PII)
| column | mask function |
|---|---|
| ssn_last_4 | governance.mask_ssn |
| date_of_birth | governance.mask_dob |
| email | governance.mask_email |
| phone | governance.mask_phone |
| address_line_1 | governance.mask_address |

### Row filter
| table | filter |
|---|---|
| silver_enrollment | governance.filter_by_lob (line-of-business scoping) |

These enforce that unauthorized roles see masked PHI/PII and only their permitted
line-of-business rows — governance applied at the platform layer, not in app code.
