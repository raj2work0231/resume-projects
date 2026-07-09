# =============================================================================
# Project 5 – OPEX Analytics
# Notebook: 02_business_rules_cost_attribution.py
#
# Purpose : Apply business rules to attribute operational expenditure to the
#           correct cost centres, asset owners, and regions. Computes derived
#           cost metrics used in Power BI dashboards.
#
# Stack   : Azure Databricks | PySpark | SparkSQL | Delta Lake
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logger = logging.getLogger("opex_business_rules")
spark  = SparkSession.builder.getOrCreate()

OPEX_BRONZE_TABLE = "catalog.bronze.v_opex_all"
COST_CENTRE_TABLE = "catalog.reference.cost_centres"
SUPPLIER_TABLE    = "catalog.reference.suppliers"
GOLD_OPEX_TABLE   = "catalog.gold.opex_cost_attribution"

# ---------------------------------------------------------------------------
# BUSINESS RULE 1: Cost Centre Attribution
# ---------------------------------------------------------------------------

def attribute_cost_centres(df: DataFrame, cost_centre_df: DataFrame) -> DataFrame:
    """
    Join OPEX records with cost centre reference to assign:
    - cost_centre_id
    - cost_centre_name
    - division
    - budget_code
    """
    return df.join(
        F.broadcast(cost_centre_df.select(
            "region", "asset_category",
            "cost_centre_id", "cost_centre_name", "division", "budget_code"
        )),
        on=["region", "asset_category"],
        how="left"
    ).withColumn(
        "cost_centre_id",
        F.coalesce(F.col("cost_centre_id"), F.lit("UNASSIGNED"))
    )

# ---------------------------------------------------------------------------
# BUSINESS RULE 2: Supplier Cost Benchmarking
# ---------------------------------------------------------------------------

def apply_supplier_benchmarking(df: DataFrame, supplier_df: DataFrame) -> DataFrame:
    """
    Enrich OPEX with supplier benchmark rates to identify over/under-spend.
    Computes:
    - benchmark_rate: expected cost per unit from supplier contract
    - cost_variance: actual cost - benchmark cost
    - cost_variance_pct: % over/under benchmark
    """
    enriched = df.join(
        F.broadcast(supplier_df.select("supplier_id", "benchmark_rate_per_unit", "contract_type")),
        on="supplier_id",
        how="left"
    )
    return (
        enriched
        .withColumn("benchmark_cost",    F.col("quantity") * F.col("benchmark_rate_per_unit"))
        .withColumn("cost_variance",     F.col("cost_amount") - F.col("benchmark_cost"))
        .withColumn("cost_variance_pct",
            F.when(F.col("benchmark_cost") > 0,
                F.round((F.col("cost_variance") / F.col("benchmark_cost")) * 100, 2)
            ).otherwise(F.lit(None))
        )
        .withColumn("is_overspend", F.col("cost_variance_pct") > 5.0)
    )

# ---------------------------------------------------------------------------
# BUSINESS RULE 3: Billing Period Normalisation
# ---------------------------------------------------------------------------

def normalise_billing_periods(df: DataFrame) -> DataFrame:
    """
    Ensure billing_period is consistently formatted as YYYY-MM.
    Handle various input formats from different supplier systems.
    """
    return (
        df
        # Attempt multiple date format parsings
        .withColumn("billing_month",
            F.coalesce(
                F.to_date(F.col("billing_period"), "yyyy-MM"),
                F.to_date(F.col("billing_period"), "MM/yyyy"),
                F.to_date(F.col("billing_period"), "yyyyMM"),
                F.to_date(F.col("billing_period"), "MMM-yyyy")
            )
        )
        .withColumn("billing_year",  F.year(F.col("billing_month")))
        .withColumn("billing_qtr",   F.quarter(F.col("billing_month")))
    )

# ---------------------------------------------------------------------------
# BUSINESS RULE 4: Flag Anomalies
# ---------------------------------------------------------------------------

def flag_anomalies(df: DataFrame) -> DataFrame:
    """
    Statistical anomaly detection using z-score within asset_category + region.
    Flags records where cost deviates > 3 std dev from the mean.
    """
    w = Window.partitionBy("asset_category", "region")

    return (
        df
        .withColumn("mean_cost", F.avg("cost_amount").over(w))
        .withColumn("std_cost",  F.stddev("cost_amount").over(w))
        .withColumn("z_score",
            F.when(F.col("std_cost") > 0,
                (F.col("cost_amount") - F.col("mean_cost")) / F.col("std_cost")
            ).otherwise(F.lit(0.0))
        )
        .withColumn("is_anomaly", F.abs(F.col("z_score")) > 3.0)
    )

# ---------------------------------------------------------------------------
# BUSINESS RULE 5: Asset Age Categorisation
# ---------------------------------------------------------------------------

def categorise_asset_age(df: DataFrame) -> DataFrame:
    """Bin assets into age categories for depreciation and replacement planning."""
    current_year = 2025
    return (
        df
        .withColumn("asset_age_years",
            F.when(F.col("install_year").isNotNull(),
                F.lit(current_year) - F.col("install_year").cast("int")
            ).otherwise(F.lit(None))
        )
        .withColumn("age_category",
            F.when(F.col("asset_age_years") < 5,  F.lit("New (0-4 yrs)"))
             .when(F.col("asset_age_years") < 10, F.lit("Mid-life (5-9 yrs)"))
             .when(F.col("asset_age_years") < 20, F.lit("Mature (10-19 yrs)"))
             .otherwise(F.lit("End-of-life (20+ yrs)"))
        )
    )

# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def run_cost_attribution(spark: SparkSession) -> None:
    logger.info("Loading OPEX Bronze data …")
    opex_df       = spark.table(OPEX_BRONZE_TABLE)
    cost_ctr_df   = spark.table(COST_CENTRE_TABLE)
    supplier_df   = spark.table(SUPPLIER_TABLE)

    logger.info("Applying business rules …")
    result = opex_df
    result = normalise_billing_periods(result)
    result = attribute_cost_centres(result, cost_ctr_df)
    result = apply_supplier_benchmarking(result, supplier_df)
    result = flag_anomalies(result)
    result = categorise_asset_age(result)
    result = result.withColumn("gold_ts", F.current_timestamp())

    logger.info(f"Writing to Gold: {GOLD_OPEX_TABLE} …")
    (
        result.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("billing_year", "asset_category")
        .saveAsTable(GOLD_OPEX_TABLE)
    )
    logger.info("Cost attribution pipeline complete.")


if __name__ == "__main__":
    run_cost_attribution(spark)
