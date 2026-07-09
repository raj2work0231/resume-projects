# =============================================================================
# Project 3 – Metadata-Driven AutoLoader Framework
# Notebook: 01_autoloader_file_notification.py
#
# Purpose : Core AutoLoader engine. Reads the dataset_config Delta table and
#           dynamically starts an AutoLoader stream per active dataset using
#           file-notification mode (Azure Event Grid + Queue Storage). Each
#           stream writes records into the centralised Multiplexer Delta table,
#           enriched with dataset_id, target_table, and CDC operation type.
#
# Stack   : Azure Databricks | AutoLoader | Delta Lake | ADLS Gen2
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("autoloader_framework")
spark  = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIG TABLE
# ---------------------------------------------------------------------------
CONFIG_TABLE          = "catalog.control.dataset_config"
FILE_TRACKING_TABLE   = "catalog.control.file_tracking"
MULTIPLEXER_TABLE     = "catalog.control.file_multiplexer"   # central routing hub

# ---------------------------------------------------------------------------
# LOAD CONFIGURATION
# ---------------------------------------------------------------------------

def load_active_configs(spark: SparkSession) -> list[dict]:
    """Load all active dataset configurations from the metadata Delta table."""
    return [
        row.asDict()
        for row in spark.table(CONFIG_TABLE).filter(F.col("is_active") == True).collect()
    ]

# ---------------------------------------------------------------------------
# DETERMINE TRIGGER TYPE FROM CONFIG
# ---------------------------------------------------------------------------

def get_trigger(mode: str):
    """
    Return Structured Streaming trigger based on config mode:
      batch    → availableNow (process all available files, then stop)
      realtime → processingTime=N seconds
    """
    if mode.lower() == "batch":
        from pyspark.sql.streaming import Trigger
        return {"availableNow": True}
    else:
        return {"processingTime": "60 seconds"}

# ---------------------------------------------------------------------------
# UPDATE FILE TRACKING TABLE
# ---------------------------------------------------------------------------

def upsert_file_tracking(file_path: str, dataset_id: str, status: str,
                          file_size: int = None, error: str = None) -> None:
    """Track file-level ingestion status in the control Delta table."""
    row = [{
        "file_path":   file_path,
        "dataset_id":  dataset_id,
        "status":      status,        # PENDING / PROCESSING / DONE / FAILED
        "file_size":   file_size or 0,
        "error":       error or "",
        "updated_at":  F.current_timestamp()
    }]
    df = spark.createDataFrame(row)
    if DeltaTable.isDeltaTable(spark, FILE_TRACKING_TABLE):
        DeltaTable.forName(spark, FILE_TRACKING_TABLE).alias("t") \
            .merge(df.alias("s"), "t.file_path = s.file_path") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df.write.format("delta").mode("append").saveAsTable(FILE_TRACKING_TABLE)

# ---------------------------------------------------------------------------
# AUTOLOADER STREAM BUILDER
# ---------------------------------------------------------------------------

def build_autoloader_stream(config: dict) -> DataFrame:
    """
    Build a cloudFiles (AutoLoader) readStream for one dataset config entry.
    """
    dataset_id    = config["dataset_id"]
    source_path   = config["source_path"]
    file_format   = config.get("file_format", "json")
    schema_path   = config.get("schema_path", f"abfss://schemas@<storage>.dfs.core.windows.net/{dataset_id}/")

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",             file_format)
        .option("cloudFiles.useNotifications",    "true")       # FILE-NOTIFICATION MODE
        .option("cloudFiles.schemaLocation",      schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes",    "true")
        .option("cloudFiles.validateOptions",     "true")
    )

    # Apply dataset-specific options from config JSON column
    extra_opts = config.get("reader_options", {}) or {}
    for k, v in extra_opts.items():
        reader = reader.option(k, str(v))

    return reader.load(source_path)

# ---------------------------------------------------------------------------
# BATCH WRITER with file tracking
# ---------------------------------------------------------------------------

def make_batch_writer(config: dict):
    """Return a foreachBatch function that enriches records and writes to the Multiplexer Delta table."""
    dataset_id    = config["dataset_id"]
    target_table  = config["target_table"]           # resolved downstream table
    operation_col = config.get("operation_col", "")  # payload column carrying I/U/D op type (optional)

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            logger.info(f"[{dataset_id}] Batch {batch_id}: empty, skipping.")
            return

        # Determine CDC operation type from payload field or default to INSERT
        if operation_col and operation_col in batch_df.columns:
            batch_df = batch_df.withColumn("operation", F.upper(F.trim(F.col(operation_col))))
        else:
            batch_df = batch_df.withColumn("operation", F.lit("INSERT"))

        enriched = (
            batch_df
            .withColumn("dataset_id",   F.lit(dataset_id))
            .withColumn("target_table", F.lit(target_table))
            .withColumn("source_file",  F.col("_metadata.file_path"))
            .withColumn("row_hash",
                F.md5(F.concat_ws("||:",
                    F.col("dataset_id"), F.col("source_file"),
                    F.to_json(F.struct(*[F.col(c) for c in batch_df.columns
                                        if c not in ("_metadata", "operation")]))
                ))
            )
            .withColumn("ingest_ts",   F.current_timestamp())
            .withColumn("ingest_date", F.to_date(F.current_timestamp()))
            .drop("_metadata")
        )

        enriched.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("ingest_date", "dataset_id") \
            .saveAsTable(MULTIPLEXER_TABLE)

        count = enriched.count()
        logger.info(f"[{dataset_id}] Batch {batch_id}: wrote {count:,} records to Multiplexer → {target_table}")

        # Update file tracking
        files = enriched.select("source_file").distinct().collect()
        for row in files:
            upsert_file_tracking(row["source_file"], dataset_id, "DONE")

    return write_batch

# ---------------------------------------------------------------------------
# START ALL STREAMS
# ---------------------------------------------------------------------------

def start_all_streams(spark: SparkSession) -> list:
    """
    Load all active dataset configs and start an AutoLoader stream for each.
    Returns list of active StreamingQuery objects.
    """
    configs = load_active_configs(spark)
    queries = []

    for config in configs:
        dataset_id  = config["dataset_id"]
        trigger_cfg = get_trigger(config.get("trigger_mode", "realtime"))
        checkpoint  = config["checkpoint_path"]
        logger.info(f"Starting AutoLoader stream for dataset: {dataset_id}")

        try:
            stream = build_autoloader_stream(config)
            writer = (
                stream.writeStream
                .foreachBatch(make_batch_writer(config))
                .option("checkpointLocation", checkpoint)
            )
            if "availableNow" in trigger_cfg:
                writer = writer.trigger(availableNow=True)
            else:
                writer = writer.trigger(processingTime=trigger_cfg["processingTime"])

            query = writer.start()
            queries.append({"dataset_id": dataset_id, "query": query})
            logger.info(f"  Stream started. Query ID: {query.id}")
        except Exception as e:
            logger.error(f"  Failed to start stream for {dataset_id}: {e}")

    return queries


# MAIN
if __name__ == "__main__":
    active_queries = start_all_streams(spark)
    logger.info(f"AutoLoader framework running: {len(active_queries)} active streams.")
    # For trigger-once batch mode, wait for all to complete:
    for q in active_queries:
        q["query"].awaitTermination()
