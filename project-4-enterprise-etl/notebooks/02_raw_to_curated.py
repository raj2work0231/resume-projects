# =============================================================================
# Project 4 – Enterprise Multi-Source ETL Pipeline
# Notebook: 02_raw_to_curated.py
#
# Purpose : Transform raw transactional data from multiple source systems
#           into curated Gold Delta tables. Applies business transformations,
#           SCD processing, and writes operational reporting views.
#
# Stack   : Azure Databricks | PySpark | Delta Lake | SparkSQL
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("raw_to_curated")
spark  = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
RAW_ORDERS_PATH     = "abfss://raw@<storage>.dfs.core.windows.net/sftp-ingest/"
RAW_ORDERS_TABLE    = "catalog.bronze.orders_raw"
SILVER_ORDERS_TABLE = "catalog.silver.orders"
GOLD_ORDERS_TABLE   = "catalog.gold.orders_curated"
GOLD_SUMMARY_TABLE  = "catalog.gold.orders_daily_summary"
ORDER_KEY           = ["order_id"]

# ---------------------------------------------------------------------------
# BRONZE INGEST — Read raw CSV/JSON files into Bronze Delta
# ---------------------------------------------------------------------------

def ingest_raw_to_bronze(spark: SparkSession) -> None:
    """Read raw CSV files from ADLS landing zone and write to Bronze Delta."""
    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(RAW_ORDERS_PATH)
    )

    enriched = (
        raw_df
        .withColumn("ingest_ts",   F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
        .withColumn("row_hash",
            F.md5(F.concat_ws("||",
                F.col("order_id"), F.col("customer_id"),
                F.col("status"), F.col("amount").cast("string")
            ))
        )
    )

    enriched.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("ingest_date") \
            .saveAsTable(RAW_ORDERS_TABLE)
    logger.info(f"Bronze ingest complete: {enriched.count():,} rows")

# ---------------------------------------------------------------------------
# SILVER TRANSFORM — Cleanse and standardise
# ---------------------------------------------------------------------------

def transform_to_silver(spark: SparkSession) -> DataFrame:
    """Apply cleansing and standardisation rules to Bronze order data."""
    df = spark.table(RAW_ORDERS_TABLE)

    # Deduplicate by order_id (keep latest by ingest_ts)
    w = Window.partitionBy("order_id").orderBy(F.col("ingest_ts").desc())
    df = df.withColumn("_rank", F.row_number().over(w)) \
           .filter(F.col("_rank") == 1).drop("_rank")

    return (
        df
        .withColumn("order_status",   F.upper(F.trim(F.col("status"))))
        .withColumn("amount",         F.round(F.col("amount").cast("double"), 2))
        .withColumn("order_date",     F.to_date(F.col("order_date_str"), "yyyy-MM-dd"))
        .withColumn("customer_id",    F.trim(F.col("customer_id")))
        .withColumn("product_id",     F.trim(F.col("product_id")))
        .withColumn("region",         F.upper(F.trim(F.col("region"))))
        .withColumn("channel",        F.upper(F.trim(F.col("channel"))))
        .withColumn("is_fulfilled",   F.col("order_status").isin("DELIVERED", "COMPLETED"))
        .withColumn("silver_ts",      F.current_timestamp())
        .drop("order_date_str", "status")
        .withColumnRenamed("order_status", "status")
    )


def write_silver_scd1(silver_df: DataFrame) -> None:
    """SCD1 upsert to Silver Delta table (latest order state)."""
    if DeltaTable.isDeltaTable(spark, SILVER_ORDERS_TABLE):
        DeltaTable.forName(spark, SILVER_ORDERS_TABLE).alias("t") \
            .merge(silver_df.alias("s"), "t.order_id = s.order_id") \
            .whenMatchedUpdate(condition="s.row_hash <> t.row_hash", set={"*": "s.*"}) \
            .whenNotMatchedInsertAll() \
            .execute()
        logger.info(f"Silver SCD1 merge complete: {silver_df.count():,} records processed")
    else:
        silver_df.write.format("delta").mode("overwrite") \
                 .option("overwriteSchema", "true") \
                 .partitionBy("order_date") \
                 .saveAsTable(SILVER_ORDERS_TABLE)
        logger.info(f"Silver table created: {silver_df.count():,} records")

# ---------------------------------------------------------------------------
# GOLD LAYER — Curated orders + daily summary
# ---------------------------------------------------------------------------

def build_curated_orders(spark: SparkSession) -> DataFrame:
    """
    Join Silver orders with reference data (customers, products) and
    compute derived columns for the Gold curated layer.
    """
    orders    = spark.table(SILVER_ORDERS_TABLE)
    customers = spark.table("catalog.reference.customers")    \
                     .select("customer_id", "customer_name", "customer_tier")
    products  = spark.table("catalog.reference.products")     \
                     .select("product_id", "product_name", "product_category")

    return (
        orders
        .join(F.broadcast(customers), on="customer_id", how="left")
        .join(F.broadcast(products),  on="product_id",  how="left")
        .withColumn("days_to_fulfil",
            F.when(F.col("is_fulfilled"),
                F.datediff(F.col("fulfilled_date"), F.col("order_date"))
            ).otherwise(F.lit(None))
        )
        .withColumn("gold_ts", F.current_timestamp())
    )


def build_daily_summary(curated_df: DataFrame) -> DataFrame:
    """Aggregate curated orders into daily business KPIs."""
    return (
        curated_df
        .groupBy("order_date", "region", "channel", "product_category", "customer_tier")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount"))).alias("completed_revenue"),
            F.sum(F.when(F.col("status") == "PENDING",   F.col("amount"))).alias("pending_revenue"),
            F.sum(F.when(F.col("status") == "FAILED",    F.lit(1))).alias("failed_orders"),
            F.avg("days_to_fulfil").alias("avg_fulfilment_days"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        .withColumn("load_ts", F.current_timestamp())
    )

# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def run_pipeline(spark: SparkSession) -> None:
    logger.info("=== Enterprise ETL Pipeline: Raw → Curated ===")

    logger.info("Step 1: Bronze ingest …")
    ingest_raw_to_bronze(spark)

    logger.info("Step 2: Silver transform …")
    silver_df = transform_to_silver(spark)
    write_silver_scd1(silver_df)

    logger.info("Step 3: Gold curated layer …")
    curated_df = build_curated_orders(spark)
    curated_df.write.format("delta").mode("overwrite") \
              .option("overwriteSchema", "false") \
              .option("mergeSchema", "true") \
              .partitionBy("order_date") \
              .saveAsTable(GOLD_ORDERS_TABLE)

    logger.info("Step 4: Daily summary …")
    summary_df = build_daily_summary(curated_df)
    summary_df.write.format("delta").mode("overwrite") \
              .partitionBy("order_date") \
              .saveAsTable(GOLD_SUMMARY_TABLE)

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline(spark)
