# =============================================================================
# Project 1 – Geo-Location Intelligence Platform
# Notebook: 01_autoloader_mediation_ingestion.py
#
# Purpose : Ingest near-real-time network event files from ADLS Gen2 using
#           Databricks AutoLoader in file-notification mode. Writes raw data
#           to the Bronze Delta layer with full audit metadata.
#
# Stack   : Azure Databricks | AutoLoader | Delta Lake | ADLS Gen2
# =============================================================================

# COMMAND ----------
# %pip install tenacity

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("autoloader_ingestion")

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION  (replace with your actual values / Databricks widgets)
# ---------------------------------------------------------------------------
SOURCE_PATH        = "abfss://raw@<storage-account>.dfs.core.windows.net/network-events/"
CHECKPOINT_PATH    = "abfss://checkpoints@<storage-account>.dfs.core.windows.net/bronze/network-events/"
BRONZE_TABLE_PATH  = "abfss://curated@<storage-account>.dfs.core.windows.net/bronze/network_events_raw/"
BRONZE_TABLE_NAME  = "catalog.bronze.network_events_raw"
TRIGGER_INTERVAL   = "30 seconds"    # near-real-time cadence
MAX_FILES_PER_TRIGGER = 500

# ---------------------------------------------------------------------------
# SCHEMA DEFINITION
# Network event file schema (illustrative – adapt to your actual schema)
# ---------------------------------------------------------------------------
network_event_schema = StructType([
    StructField("event_id",        StringType(),    True),
    StructField("device_id",       StringType(),    True),
    StructField("cell_site_id",    StringType(),    True),
    StructField("latitude",        DoubleType(),    True),
    StructField("longitude",       DoubleType(),    True),
    StructField("event_type",      StringType(),    True),
    StructField("signal_strength", DoubleType(),    True),
    StructField("event_ts",        TimestampType(), True),
    StructField("technology",      StringType(),    True),   # 4G / 5G / etc.
    StructField("data_usage_mb",   DoubleType(),    True),
])

# COMMAND ----------
# ---------------------------------------------------------------------------
# AUTOLOADER STREAM — FILE NOTIFICATION MODE
# ---------------------------------------------------------------------------
# file-notification mode uses Azure Event Grid + Queue Storage to receive
# file-arrival notifications instead of listing the source directory,
# making it highly scalable for large landing zones.

def read_stream_autoloader() -> "DataFrame":
    """
    Configure Databricks AutoLoader with file-notification mode.
    Returns a streaming DataFrame.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")                # or "csv", "parquet"
        .option("cloudFiles.useNotifications", "true")      # FILE-NOTIFICATION MODE
        .option("cloudFiles.validateOptions", "true")
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "_schema/")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(network_event_schema)
        .load(SOURCE_PATH)
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# TRANSFORMATION — ADD AUDIT METADATA
# ---------------------------------------------------------------------------

def enrich_with_metadata(df: "DataFrame") -> "DataFrame":
    """
    Attach pipeline audit columns to every incoming record.
    """
    return (
        df
        # MD5 row hash for downstream change detection
        .withColumn(
            "row_hash",
            F.md5(F.concat_ws("||",
                F.col("event_id"), F.col("device_id"),
                F.col("cell_site_id"), F.col("event_ts")
            ))
        )
        # Source file path from AutoLoader metadata
        .withColumn("source_file",     F.col("_metadata.file_path"))
        .withColumn("source_modified", F.col("_metadata.file_modification_time"))
        # Pipeline audit timestamps
        .withColumn("ingest_ts",   F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
        # Drop AutoLoader internal metadata column after extraction
        .drop("_metadata")
    )


# COMMAND ----------
# ---------------------------------------------------------------------------
# WRITE STREAM — DELTA BRONZE TABLE
# ---------------------------------------------------------------------------

def write_bronze(stream_df: "DataFrame"):
    """
    Write enriched stream to Bronze Delta table.
    Uses append mode; idempotency guaranteed by AutoLoader checkpointing.
    """
    return (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")           # allow schema evolution
        .partitionBy("ingest_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .toTable(BRONZE_TABLE_NAME)
    )


# COMMAND ----------
# ---------------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------------

logger.info("Starting AutoLoader ingestion stream …")

raw_stream  = read_stream_autoloader()
rich_stream = enrich_with_metadata(raw_stream)
query       = write_bronze(rich_stream)

logger.info(f"Stream started. Query ID: {query.id}")
logger.info(f"Checkpoint: {CHECKPOINT_PATH}")
logger.info(f"Target: {BRONZE_TABLE_NAME}")

# For batch/trigger-once runs (e.g., in Databricks Workflows):
# query.awaitTermination()

# COMMAND ----------
# ---------------------------------------------------------------------------
# UTILITY — BRONZE TABLE HEALTH CHECK
# ---------------------------------------------------------------------------

def bronze_health_check(table_name: str) -> None:
    """Quick validation of the Bronze table after load."""
    df = spark.table(table_name)
    total     = df.count()
    nulls     = df.filter(F.col("event_id").isNull()).count()
    dupes     = total - df.dropDuplicates(["event_id"]).count()
    latest_ts = df.agg(F.max("ingest_ts")).collect()[0][0]

    print(f"=== Bronze Health Check: {table_name} ===")
    print(f"  Total records  : {total:,}")
    print(f"  Null event_ids : {nulls:,}")
    print(f"  Duplicates     : {dupes:,}")
    print(f"  Latest ingest  : {latest_ts}")


# Uncomment for interactive validation:
# bronze_health_check(BRONZE_TABLE_NAME)
