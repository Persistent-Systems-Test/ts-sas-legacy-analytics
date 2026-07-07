# Insurance Pipeline Migration

Reference migration of the legacy SAS **insurance** analytics pipeline
(`Programs/Insurance/`) to modern targets. This is a worked example of moving the
SAS estate described in the repo root README to dbt/Snowflake and
PySpark/Databricks.

## Contents

| Path | What it is |
|------|-----------|
| `analysis/insurance_pipeline_analysis.md` | Migration analysis: source system, extracted business rules, data lineage, SAS→target construct mapping, parity notes. |
| `dbt/` | dbt project (Snowflake target) implementing `claims_processing.sas` and `policy_valuation.sas`. |
| `pyspark/` | PySpark/Databricks equivalents of the same two pipelines. |

## Source → target coverage

| SAS program | dbt models | PySpark |
|-------------|-----------|---------|
| `claims_processing.sas` | `int_claims_validated` → `int_claims_fraud_scored` → `int_claims_adjudicated` → `claims_register` / `claims_review_queue` / `fraud_alerts` | `pyspark/claims_processing.py` |
| `policy_valuation.sas` | `int_policy_inforce` / `int_policy_claims_experience` / `int_policy_premium_collections` → `policy_valuation` → `loss_ratio_summary` | `pyspark/policy_valuation.py` |
| `insurance_formats.sas` | dbt seeds (`seed_policy_type`, `seed_claim_status`, `seed_risk_category`, `seed_coverage_type`) | broadcast lookups |

## Running the dbt project

```bash
cd migration/dbt
cp profiles.example.yml ~/.dbt/profiles.yml   # then fill in Snowflake creds
dbt deps        # if you add packages
dbt seed
dbt run  --vars '{"proc_date": "2024-01-15", "valuation_date": "2024-01-31", "lob": "ALL"}'
dbt test
```

The `proc_date`, `valuation_date`, and `lob` vars mirror the SAS macro parameters
(`&CURR_DT`, `val_date`, `lob=`).

## Running the PySpark jobs

```bash
cd migration/pyspark
spark-submit claims_processing.py --proc-date 2024-01-15
spark-submit policy_valuation.py  --val-date 2024-01-31 --lob ALL
```

On Databricks, import the two files as a job with two tasks (daily claims,
monthly valuation) and wire the SIU fraud alert to a Workflows notification task
(replacing the SAS `%sendmail`).

## Notes

- Business rules (validation, fraud banding, auto-adjudication order, valuation
  formulas) are documented and cross-referenced in `analysis/`.
- Both targets make the pipelines **idempotent** per run date (the SAS
  `PROC APPEND` was not) so re-runs of a date don't double-count.
- These artifacts model the target design; they expect the migrated
  landing/warehouse tables to exist (see `dbt/models/staging/_sources.yml`).
