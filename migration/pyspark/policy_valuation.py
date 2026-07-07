"""Databricks / PySpark port of Programs/Insurance/policy_valuation.sas.

Monthly in-force book valuation: earned premium, 12-month claims experience,
premium collections, loss/combined ratios, IBNR and reserves, plus a
loss-ratio summary by line of business.

    spark-submit policy_valuation.py --val-date 2024-01-31 --lob ALL
"""
from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession, functions as F


SRC_POLICIES = "raw_insurance.policies"
SRC_CLAIMS = "raw_insurance.claims"
SRC_PREMIUMS = "raw_insurance.premiums"
TGT_VALUATION = "stg_insurance.policy_valuation"
TGT_LOSS_RATIO_SUMMARY = "reports.loss_ratio_summary"

OPEN_STATUSES = ("OPEN", "INV", "ADJ", "PEND")


def inforce_policies(policies: DataFrame, val_date: str, lob: str) -> DataFrame:
    """Step 1: in-force policies + earned premium (SAS WORK.INFORCE)."""
    vd = F.lit(val_date).cast("date")
    df = policies.filter(
        (F.upper("status") == "ACTIVE")
        & (F.col("effective_date") <= vd)
        & (F.col("expiration_date") >= vd)
    )
    if lob != "ALL":
        df = df.filter(F.col("policy_type") == lob)

    year_start = F.trunc(vd, "year")
    months_earned = F.months_between(
        F.least(vd, F.col("expiration_date")),
        F.greatest(F.col("effective_date"), year_start),
    ).cast("int")

    return (
        df.withColumn(
            "policy_age_months",
            F.months_between(vd, F.col("effective_date")).cast("int"),
        )
        .withColumn(
            "months_to_expiry",
            F.months_between(F.col("expiration_date"), vd).cast("int"),
        )
        .withColumn(
            "renewal_due_flag",
            F.when(F.col("expiration_date") <= F.add_months(vd, 3), F.lit("Y")).otherwise(
                F.lit("N")
            ),
        )
        .withColumn(
            "ytd_earned_premium",
            F.col("annual_premium") / 12 * F.least(F.lit(12), months_earned),
        )
    )


def claims_experience(claims: DataFrame, val_date: str) -> DataFrame:
    """Step 2: 12-month claims experience per policy (SAS WORK.CLAIMS_EXP)."""
    vd = F.lit(val_date).cast("date")
    windowed = claims.filter(
        (F.col("loss_date") >= F.add_months(vd, -12)) & (F.col("loss_date") <= vd)
    )
    open_reserve = F.when(
        F.col("claim_status").isin(*OPEN_STATUSES), F.col("reserved_amount")
    ).otherwise(F.lit(0))
    return windowed.groupBy("policy_id").agg(
        F.countDistinct("claim_id").alias("num_claims"),
        F.sum("incurred_amount").alias("total_incurred"),
        F.sum("paid_amount").alias("total_paid"),
        F.sum("reserved_amount").alias("total_reserved"),
        F.max("loss_date").alias("last_claim_date"),
        F.sum(open_reserve).alias("open_reserves"),
        F.sum(F.when(F.col("claim_status") == "DENY", 1).otherwise(0)).alias(
            "denied_claims"
        ),
    )


def premium_collections(premiums: DataFrame, val_date: str) -> DataFrame:
    """Step 3: YTD premium collections per policy (SAS WORK.PREMIUM_COLL)."""
    vd = F.lit(val_date).cast("date")
    ytd = premiums.filter(
        (F.col("payment_date") >= F.trunc(vd, "year")) & (F.col("payment_date") <= vd)
    )
    return ytd.groupBy("policy_id").agg(
        F.sum("premium_amount").alias("collected_premium"),
        F.sum(
            F.when(
                F.upper("payment_status") == "RETURNED", F.col("premium_amount")
            ).otherwise(0)
        ).alias("returned_premium"),
        F.max("payment_date").alias("last_payment_date"),
        F.count(F.when(F.upper("payment_status") == "LATE", 1)).alias("late_payments"),
    )


def valuation(inforce: DataFrame, exp: DataFrame, prem: DataFrame, val_date: str) -> DataFrame:
    """Step 4: merge + valuation metrics (SAS STG_INS.POLICY_VALUATION)."""
    df = inforce.join(exp, "policy_id", "left").join(prem, "policy_id", "left")

    loss_ratio = F.when(
        F.col("ytd_earned_premium") > 0,
        F.coalesce(F.col("total_incurred"), F.lit(0)) / F.col("ytd_earned_premium"),
    ).otherwise(F.lit(None))
    df = df.withColumn("loss_ratio", loss_ratio)

    combined = F.when(
        F.col("loss_ratio").isNotNull(), F.col("loss_ratio") + 0.30
    ).otherwise(F.lit(None))
    df = df.withColumn("combined_ratio", combined)

    ibnr = F.greatest(
        F.lit(0),
        F.col("ytd_earned_premium") * 0.15 - F.coalesce(F.col("total_paid"), F.lit(0)),
    )
    return (
        df.withColumn(
            "premium_adequate",
            F.when(F.col("combined_ratio").isNull(), F.lit("N"))
            .when(F.col("combined_ratio") > 1.0, F.lit("N"))
            .otherwise(F.lit("Y")),
        )
        .withColumn("ibnr_estimate", ibnr)
        .withColumn(
            "total_reserve", F.coalesce(F.col("open_reserves"), F.lit(0)) + ibnr
        )
        .withColumn("valuation_date", F.lit(val_date).cast("date"))
    )


def loss_ratio_summary(val_df: DataFrame) -> DataFrame:
    """Step 5: loss/combined ratios by line of business (SAS REPORTS.LOSS_RATIO_SUMMARY)."""
    summary = val_df.groupBy("policy_type").agg(
        F.count("*").alias("n_policies"),
        F.sum("ytd_earned_premium").alias("total_earned"),
        F.sum("total_incurred").alias("total_incurred"),
        F.sum("total_paid").alias("total_paid"),
        F.sum("total_reserve").alias("total_reserves"),
        F.sum("ibnr_estimate").alias("total_ibnr"),
    )
    agg_lr = F.when(
        F.col("total_earned") > 0, F.col("total_incurred") / F.col("total_earned")
    ).otherwise(F.lit(None))
    return summary.withColumn("agg_loss_ratio", agg_lr).withColumn(
        "agg_combined_ratio",
        F.when(F.col("total_earned") > 0, agg_lr + 0.30).otherwise(F.lit(None)),
    )


def run(spark: SparkSession, val_date: str, lob: str) -> None:
    inforce = inforce_policies(spark.table(SRC_POLICIES), val_date, lob)
    exp = claims_experience(spark.table(SRC_CLAIMS), val_date)
    prem = premium_collections(spark.table(SRC_PREMIUMS), val_date)

    val_df = valuation(inforce, exp, prem, val_date)
    summary = loss_ratio_summary(val_df)

    _overwrite_partition(val_df, TGT_VALUATION, val_date, "valuation_date")
    summary.write.format("delta").mode("overwrite").saveAsTable(TGT_LOSS_RATIO_SUMMARY)

    print(f"NOTE: policy_valuation completed for {val_date} (lob={lob})")
    print(f"NOTE: policies valued: {val_df.count()}")


def _overwrite_partition(df: DataFrame, table: str, val_date: str, date_col: str) -> None:
    if df.sparkSession.catalog.tableExists(table):
        df.sparkSession.sql(f"DELETE FROM {table} WHERE {date_col} = DATE'{val_date}'")
    df.write.format("delta").mode("append").saveAsTable(table)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Monthly policy valuation")
    p.add_argument("--val-date", default=date.today().isoformat())
    p.add_argument(
        "--lob",
        default="ALL",
        choices=["ALL", "AUTO", "HOME", "WL", "TL", "UL", "HLTH"],
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    spark_session = SparkSession.builder.appName("policy_valuation").getOrCreate()
    run(spark_session, args.val_date, args.lob)
