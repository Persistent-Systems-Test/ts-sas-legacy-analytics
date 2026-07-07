# PySpark / Databricks jobs

PySpark ports of the insurance SAS pipelines. Each module exposes small, testable
transform functions plus a `run(spark, ...)` entry point.

| File | SAS source | Entry point |
|------|-----------|-------------|
| `claims_processing.py` | `Programs/Insurance/claims_processing.sas` | `run(spark, proc_date)` |
| `policy_valuation.py` | `Programs/Insurance/policy_valuation.sas` | `run(spark, val_date, lob)` |

## Local run

```bash
pip install "pyspark>=3.4"
spark-submit claims_processing.py --proc-date 2024-01-15
spark-submit policy_valuation.py  --val-date 2024-01-31 --lob ALL
```

## Databricks

Import as a job with two tasks. The transform functions
(`validate_claims`, `fraud_score`, `adjudicate`, `inforce_policies`, ...) are
pure DataFrame→DataFrame and can be unit-tested with a local SparkSession.

Table names are module-level constants (`raw_insurance.*`, `raw_dw.*`,
`stg_insurance.*`, `reports.*`) — point them at your Unity Catalog schemas.

The SAS `%sendmail` SIU alert is represented by `_emit_siu_alert`; replace it
with a Databricks Workflows notification / email task.
