# =============================================================================
# Project 2 – Real-Time Kafka Streaming Pipeline
# Notebook: 02_parquet_to_multiplexer.py
#
# Purpose : Read raw Parquet files from ADLS Gen2 landing zone via AutoLoader
#           (cloudFiles) and stream records into the centralised Multiplexer
#           Delta table. Each record is enriched with:
#             - kafka_topic   : source Kafka topic (from file path / payload)
#             - target_table  : downstream Delta table (from routing config)
#             - operation     : CDC op type — INSERT / UPDATE / DELETE
#           The Multiplexer acts as a universal CDC router for all topics.
#
# Stack   : Azure Databricks | AutoLoader | PySpark | Delta Lake
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, BooleanType
import logging

logger = logging.getLogger("parquet_to_multiplexer")
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
PARQUET_LANDING_BASE  = "abfss://raw@<storage>.dfs.core.windows.net/kafka-landing/"
MULTIPLEXER_TABLE     = "catalog.bronze.kafka_multiplexer"
ROUTING_CONFIG_TABLE  = "catalog.config.topic_routing"
CHECKPOINT_PATH       = "abfss://checkpoints@<storage>.dfs.core.windows.net/multiplexer/"
TRIGGER_INTERVAL      = "30 seconds"

# ---------------------------------------------------------------------------
# STEP 1 — Load routing config (broadcast for efficiency)
# ---------------------------------------------------------------------------

def load_routing_config(spark: SparkSession) -> DataFrame:
    """
    Load topic_routing config as a broadcast DataFrame.
    Schema: kafka_topic, target_table, scd_type, business_key_cols, is_active
    """
    return spark.table(ROUTING_CONFIG_TABLE).filter(F.col("is_active") == True)

# ---------------------------------------------------------------------------
# STEP 2 — AutoLoader: stream Parquet files from landing zone
# ---------------------------------------------------------------------------

def read_parquet_landing(spark: SparkSession) -> DataFrame:
    """
    Use AutoLoader (cloudFiles) to incrementally read new Parquet files from
    the ADLS Gen2 landing zone. File-notification mode for low-latency discovery.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",            "parquet")
        .option("cloudFiles.useNotifications",  "true")
        .option("cloudFiles.schemaLocation",    CHECKPOINT_PATH + "_schema")
        .option("cloudFiles.inferColumnTypes",  "true")
        .option("recursiveFileLookup",          "true")
        .load(PARQUET_LANDING_BASE)
    )

# ---------------------------------------------------------------------------
# STEP 3 — Enrich with Multiplexer columns
# ---------------------------------------------------------------------------

def enrich_for_multiplexer(df: DataFrame, routing_df: DataFrame) -> DataFrame:
    """
    Enrich each record with:
      - operation    : CDC type from payload op_type field (default INSERT)
      - target_table : resolved from topic_routing config via kafka_topic
      - row_hash     : MD5 of payload for change detection
      - processed_flag: False — set to True by downstream routing job after MERGE
    """
    # Resolve target_table from routing config (broadcast join on kafka_topic)
    routing_bc = routing_df.select("kafka_topic", "target_table", "scd_type", "business_key_cols")

    enriched = (
        df
        # Extract CDC operation type from payload (INSERT / UPDATE / DELETE); default INSERT
        .withColumn("operation",
            F.upper(F.coalesce(F.col("op_type"), F.lit("INSERT")))
        )
        # Row hash over the full payload value for change detection
        .withColumn("row_hash",
            F.md5(F.coalesce(F.col("value_str"), F.lit("")))
        )
        # Mark unprocessed — downstream routing job flips this to True after MERGE
        .withColumn("processed_flag", F.lit(False))
        .withColumn("multiplexer_ingest_ts", F.current_timestamp())
        # Join routing config to resolve target_table and scd_type
        .join(F.broadcast(routing_bc), on="kafka_topic", how="left")
    )
    return enriched

# ---------------------------------------------------------------------------
# STEP 4 — Write to Multiplexer Delta Table
# ---------------------------------------------------------------------------

def write_multiplexer_stream(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch handler: write enriched records to Multiplexer Delta table.
    Unroutable records (no matching topic in config) are logged as warnings.
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping.")
        return

    # Warn on records with no target_table resolved
    unroutable = batch_df.filter(F.col("target_table").isNull())
    if unroutable.count() > 0:
        logger.warning(f"Batch {batch_id}: {unroutable.count()} records have no routing config entry.")

    routable = batch_df.filter(F.col("target_table").isNotNull())

    routable.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(MULTIPLEXER_TABLE)

    logger.info(f"Batch {batch_id}: {routable.count()} records written to Multiplexer.")

# ---------------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting Parquet landing → Multiplexer streaming job …")

    routing_df  = load_routing_config(spark)
    landing_df  = read_parquet_landing(spark)
    enriched_df = enrich_for_multiplexer(landing_df, routing_df)

    query = (
        enriched_df.writeStream
        .foreachBatch(write_multiplexer_stream)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(f"Multiplexer stream running. Query ID: {query.id}")
    # query.awaitTermination()


main()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("bronze_to_silver")
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
BRONZE_TABLE          = "catalog.bronze.transactions_raw"
SILVER_TABLE          = "catalog.silver.transactions"
CHECKPOINT_PATH       = "abfss://checkpoints@<storage>.dfs.core.windows.net/silver/transactions/"
BUSINESS_KEY          = ["transaction_id"]
TRIGGER_INTERVAL      = "60 seconds"
DQ_THRESHOLD_PCT      = 95.0      # Minimum acceptable data quality %

# ---------------------------------------------------------------------------
# VALIDATION RULES
# ---------------------------------------------------------------------------

VALIDATION_RULES = {
    "transaction_id_not_null": F.col("transaction_id").isNotNull(),
    "account_id_not_null":     F.col("account_id").isNotNull(),
    "amount_positive":         F.col("amount") > 0,
    "valid_currency":          F.col("currency").isin("USD", "CAD", "EUR", "GBP"),
    "valid_status":            F.col("status").isin("PENDING", "COMPLETED", "FAILED", "REVERSED"),
    "event_ts_not_null":       F.col("event_ts").isNotNull(),
    "event_ts_not_future":     F.col("event_ts") <= F.current_timestamp(),
}


def apply_validation(df: DataFrame) -> tuple:
    """
    Apply all validation rules. Returns (valid_df, invalid_df, dq_score).
    Invalid records carry a failure_reason column.
    """
    combined_rule = F.lit(True)
    failure_expr  = F.lit("")

    for rule_name, rule_expr in VALIDATION_RULES.items():
        combined_rule = combined_rule & rule_expr
        failure_expr  = F.when(~rule_expr, F.concat(failure_expr, F.lit(f"|{rule_name}"))).otherwise(failure_expr)

    df_checked = df.withColumn("is_valid",       combined_rule) \
                   .withColumn("failure_reason",  failure_expr.substr(2, 1000))  # strip leading |

    valid_df   = df_checked.filter(F.col("is_valid")).drop("is_valid", "failure_reason")
    invalid_df = df_checked.filter(~F.col("is_valid")).drop("is_valid")

    total   = df.count()
    valid   = valid_df.count()
    dq_score = (valid / total * 100) if total > 0 else 0.0
    logger.info(f"DQ check: {valid}/{total} valid ({dq_score:.1f}%)")

    return valid_df, invalid_df, dq_score

# COMMAND ----------
# ---------------------------------------------------------------------------
# STANDARDISATION
# ---------------------------------------------------------------------------

def standardise(df: DataFrame) -> DataFrame:
    """
    Clean and standardise data types, string formats, and derived columns.
    """
    return (
        df
        # Normalise string fields
        .withColumn("transaction_type", F.upper(F.trim(F.col("transaction_type"))))
        .withColumn("status",           F.upper(F.trim(F.col("status"))))
        .withColumn("currency",         F.upper(F.trim(F.col("currency"))))
        .withColumn("channel",          F.upper(F.trim(F.col("channel"))))
        # Derive event date for partitioning
        .withColumn("event_date",       F.to_date(F.col("event_ts")))
        # Round monetary amount to 2 decimal places
        .withColumn("amount",           F.round(F.col("amount"), 2))
        # Add pipeline silver metadata
        .withColumn("silver_processed_ts", F.current_timestamp())
        .withColumn("row_hash",
            F.md5(F.concat_ws("||",
                F.col("transaction_id"), F.col("account_id"),
                F.col("amount").cast("string"), F.col("status"), F.col("event_ts").cast("string")
            ))
        )
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# DEDUPLICATION — keep latest record per business key per micro-batch
# ---------------------------------------------------------------------------

def deduplicate(df: DataFrame, key_cols: list, order_col: str = "event_ts") -> DataFrame:
    """
    Within a micro-batch: deduplicate by business key, keeping the latest
    record based on the specified order column.
    """
    from pyspark.sql.window import Window
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# SCD TYPE 1 MERGE — Silver table (latest-record-wins)
# ---------------------------------------------------------------------------

def merge_to_silver_scd1(batch_df: DataFrame, batch_id: int) -> None:
    """
    Merge incoming batch into Silver Delta table using SCD Type 1.
    On match: update if row_hash has changed (record is newer).
    On no match: insert as new record.
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping merge.")
        return

    # 1. Validate
    valid_df, invalid_df, dq_score = apply_validation(batch_df)

    # 2. Quarantine invalid records
    if invalid_df.count() > 0:
        invalid_df.write.format("delta").mode("append") \
                  .saveAsTable("catalog.silver.transactions_quarantine")

    # 3. Alert if DQ drops below threshold
    if dq_score < DQ_THRESHOLD_PCT:
        logger.error(f"Batch {batch_id}: DQ score {dq_score:.1f}% below threshold {DQ_THRESHOLD_PCT}%!")
        # In production: raise alert / send SMTP notification

    # 4. Standardise + deduplicate
    clean_df = standardise(valid_df)
    clean_df = deduplicate(clean_df, BUSINESS_KEY)

    # 5. SCD1 merge
    if DeltaTable.isDeltaTable(spark, SILVER_TABLE):
        delta_target = DeltaTable.forName(spark, SILVER_TABLE)
        (
            delta_target.alias("t")
            .merge(clean_df.alias("s"), "t.transaction_id = s.transaction_id")
            .whenMatchedUpdate(
                condition="s.row_hash <> t.row_hash",   # update only if changed
                set={"*": "s.*"}
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First run — create table
        clean_df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy("event_date") \
                .saveAsTable(SILVER_TABLE)

    logger.info(f"Batch {batch_id}: Silver merge complete. DQ={dq_score:.1f}%")

# COMMAND ----------
# ---------------------------------------------------------------------------
# WRITE DQ METRICS
# ---------------------------------------------------------------------------

def log_dq_metrics(batch_id: int, topic: str, total: int, valid: int, dq_score: float) -> None:
    """Write DQ metrics to monitoring table for dashboards and alerting."""
    metrics = [{
        "batch_id":   batch_id,
        "topic":      topic,
        "total":      total,
        "valid":      valid,
        "invalid":    total - valid,
        "dq_score":   dq_score,
        "metric_ts":  F.current_timestamp()
    }]
    spark.createDataFrame(metrics) \
         .write.format("delta").mode("append") \
         .saveAsTable("catalog.monitoring.dq_metrics")

# COMMAND ----------
# ---------------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting Bronze → Silver streaming job …")

    bronze_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")     # handle Delta CDF updates
        .table(BRONZE_TABLE)
    )

    query = (
        bronze_stream.writeStream
        .foreachBatch(merge_to_silver_scd1)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(f"Silver stream query ID: {query.id}")
    # query.awaitTermination()


main()
