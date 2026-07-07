"""Databricks / PySpark port of Programs/Insurance/claims_processing.sas.

Daily claims intake: validate against active policies, fraud-screen, auto-adjudicate,
and write the claims register, manual-review queue, and fraud alerts.

Run on Databricks (job or notebook) or locally with a SparkSession:

    spark-submit claims_processing.py --proc-date 2024-01-15

Reads/writes Delta tables that replace the SAS RAW_INS / TERA_DW / STG_INS libraries.
"""
from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession, functions as F


# Table names (override via env/conf for other catalogs/schemas).
SRC_CLAIMS_FEED = "raw_insurance.claims_feed"
SRC_POLICIES = "raw_insurance.policies"
SRC_FRAUD_INDICATORS = "raw_dw.fraud_indicators"
TGT_CLAIMS_REGISTER = "stg_insurance.claims_register"
TGT_REVIEW_QUEUE = "stg_insurance.claims_review_queue"
TGT_FRAUD_ALERTS = "stg_insurance.fraud_alerts"


def validate_claims(feed: DataFrame, policies: DataFrame) -> DataFrame:
    """Step 1: validate feed claims against ACTIVE policies (SAS hash lookup).

    Returns feed rows enriched with policy attrs, an ``is_valid`` flag, and a
    ``validation_error`` message for rejects.
    """
    active = policies.filter(F.upper("status") == "ACTIVE").select(
        "policy_id",
        "policy_type",
        "effective_date",
        "expiration_date",
        "sum_insured",
        "deductible",
    )

    joined = feed.join(F.broadcast(active), on="policy_id", how="left")

    validation_error = (
        F.when(
            F.col("policy_type").isNull(),
            F.concat(F.lit("Policy not found or inactive: "), F.col("policy_id")),
        )
        .when(
            (F.col("loss_date") < F.col("effective_date"))
            | (F.col("loss_date") > F.col("expiration_date")),
            F.concat(
                F.lit("Loss date "),
                F.date_format("loss_date", "ddMMMyyyy"),
                F.lit(" outside policy period "),
                F.date_format("effective_date", "ddMMMyyyy"),
                F.lit(" - "),
                F.date_format("expiration_date", "ddMMMyyyy"),
            ),
        )
        .when(
            F.col("claimed_amount") > F.col("sum_insured"),
            F.concat(
                F.lit("Claimed amount "),
                F.format_number("claimed_amount", 2),
                F.lit(" exceeds sum insured "),
                F.format_number("sum_insured", 2),
            ),
        )
        .otherwise(F.lit(None))
    )

    return joined.withColumn("validation_error", validation_error).withColumn(
        "is_valid", F.col("validation_error").isNull()
    )


def fraud_score(valid_claims: DataFrame, fraud: DataFrame) -> DataFrame:
    """Step 2: attach fraud indicators and band the risk (SAS FRAUD_CHECK)."""
    scored = valid_claims.join(
        F.broadcast(fraud), on=["policy_id", "claimant_id"], how="left"
    )
    return scored.withColumn(
        "fraud_risk",
        F.when(F.col("fraud_score") >= 80, F.lit("HIGH"))
        .when(F.col("fraud_score") >= 50, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )


def adjudicate(scored: DataFrame) -> DataFrame:
    """Step 3: ordered auto-adjudication rules (SAS if/return precedence)."""
    result = (
        F.when(F.col("fraud_risk") == "HIGH", F.lit("DENY"))
        .when(
            (F.col("fraud_risk") == "LOW")
            & (F.col("claimed_amount") <= 5000)
            & (F.col("policy_type").isin("AUTO", "HOME", "RENT")),
            F.lit("APPR"),
        )
        .when(
            (F.col("fraud_risk") == "LOW")
            & (F.col("claimed_amount") <= F.col("sum_insured") * 0.25)
            & (F.col("claimed_amount") <= 50000),
            F.lit("APPR"),
        )
        .otherwise(F.lit("PEND"))
    )
    df = scored.withColumn("adjudication_result", result)

    reason = (
        F.when(
            F.col("adjudication_result") == "DENY",
            F.lit("High fraud risk - SIU referral"),
        )
        .when(
            (F.col("adjudication_result") == "APPR")
            & (F.col("claimed_amount") <= 5000)
            & (F.col("policy_type").isin("AUTO", "HOME", "RENT")),
            F.lit("Auto-approved: low risk, small claim"),
        )
        .when(
            F.col("adjudication_result") == "APPR",
            F.lit("Auto-approved: within 25% of sum insured"),
        )
        .otherwise(
            F.trim(
                F.concat(
                    F.when(
                        F.col("fraud_risk") == "MEDIUM", F.lit("Medium fraud risk; ")
                    ).otherwise(F.lit("")),
                    F.when(
                        F.col("claimed_amount") > 50000, F.lit("Large claim; ")
                    ).otherwise(F.lit("")),
                    F.when(
                        F.col("claimed_amount") > F.col("sum_insured") * 0.25,
                        F.lit("Exceeds 25% threshold"),
                    ).otherwise(F.lit("")),
                )
            )
        )
    )

    approved = (
        F.when(F.col("adjudication_result") == "DENY", F.lit(0.0))
        .when(
            F.col("adjudication_result") == "APPR",
            F.greatest(F.lit(0.0), F.col("claimed_amount") - F.col("deductible")),
        )
        .otherwise(F.lit(None).cast("double"))
    )

    return (
        df.withColumn("adjudication_reason", reason)
        .withColumn("approved_amount", approved)
        .withColumn(
            "is_manual_review", F.col("adjudication_result").isin("DENY", "PEND")
        )
    )


def run(spark: SparkSession, proc_date: str) -> None:
    feed = spark.table(SRC_CLAIMS_FEED).where(F.col("feed_date") == F.lit(proc_date))
    policies = spark.table(SRC_POLICIES)
    fraud = spark.table(SRC_FRAUD_INDICATORS)

    validated = validate_claims(feed, policies)
    valid = validated.filter("is_valid")

    scored = fraud_score(valid, fraud)
    adjudicated = adjudicate(scored).withColumn(
        "processing_date", F.lit(proc_date).cast("date")
    )

    register = adjudicated.withColumn(
        "claim_status", F.col("adjudication_result")
    )
    review_queue = adjudicated.filter("is_manual_review")
    fraud_alerts = scored.filter(F.col("fraud_risk") == "HIGH").withColumn(
        "alert_reason",
        F.concat(
            F.lit("Fraud score: "),
            F.col("fraud_score").cast("string"),
            F.lit("; "),
            F.coalesce(F.col("indicator_flags"), F.lit("")),
        ),
    ).withColumn("alert_date", F.lit(proc_date).cast("date"))

    _append(register, TGT_CLAIMS_REGISTER, proc_date, "processing_date")
    _append(review_queue, TGT_REVIEW_QUEUE, proc_date, "processing_date")
    _append(fraud_alerts, TGT_FRAUD_ALERTS, proc_date, "alert_date")

    n_fraud = fraud_alerts.count()
    print(f"NOTE: claims_processing completed for {proc_date}")
    print(f"NOTE: auto-adjudicated: {register.filter(~F.col('is_manual_review')).count()}")
    print(f"NOTE: manual review: {review_queue.count()}")
    print(f"NOTE: fraud alerts: {n_fraud}")
    if n_fraud > 0:
        # SAS %sendmail SIU alert -> orchestration/alerting hook.
        _emit_siu_alert(proc_date, n_fraud)


def _append(df: DataFrame, table: str, proc_date: str, date_col: str) -> None:
    """Idempotent append: delete the partition for proc_date, then append.

    Replaces SAS PROC APPEND (non-idempotent) with a re-runnable Delta write.
    """
    if df.sql_ctx.sparkSession.catalog.tableExists(table):
        df.sparkSession.sql(
            f"DELETE FROM {table} WHERE {date_col} = DATE'{proc_date}'"
        )
    (df.write.format("delta").mode("append").saveAsTable(table))


def _emit_siu_alert(proc_date: str, n_fraud: int) -> None:
    # Placeholder for Databricks Workflows alert / email task.
    print(f"ALERT: SIU — {n_fraud} high-risk claims on {proc_date}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Daily claims processing")
    p.add_argument("--proc-date", default=date.today().isoformat())
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    spark_session = SparkSession.builder.appName("claims_processing").getOrCreate()
    run(spark_session, args.proc_date)
