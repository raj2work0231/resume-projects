# =============================================================================
# Project 3 – AutoLoader File-Notification Multiplexer Pipeline
# Notebook: 03_streaming_scd_downstream.py
#
# Purpose : Start a dedicated streaming job per active dataset that reads from
#           the Multiplexer Delta table (filtered by dataset_id) and applies
#           SCD Type 1 or SCD Type 2 MERGE to the downstream Silver table via
#           foreachBatch. Multiple datasets run as parallel StreamingQueries
#           with independent checkpoints, enabling near-real-time propagation.
#
# Stack   : Azure Databricks | PySpark | Spark Structured Streaming | Delta Lake
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("streaming_scd_downstream")
spark  = SparkSession.builder.getOrCreate()

CONFIG_TABLE      = "catalog.control.dataset_config"
MULTIPLEXER_TABLE = "catalog.control.file_multiplexer"
HIGH_DATE         = "9999-12-31T23:59:59"

# ---------------------------------------------------------------------------
# LOAD ACTIVE DATASET CONFIGS
# ---------------------------------------------------------------------------

def load_active_configs(spark: SparkSession) -> list:
    """Load all active dataset configs from the metadata Delta table."""
    return [
        row.asDict()
        for row in spark.table(CONFIG_TABLE).filter(F.col("is_active") == True).collect()
    ]

# ---------------------------------------------------------------------------
# SCD TYPE 1 — latest-record-wins MERGE
# ---------------------------------------------------------------------------

def apply_scd1(
    batch_df: DataFrame,
    target_table: str,
    business_key: list,
    hash_cols: list
) -> None:
    """SCD1: on key match update if row_hash changed; on no match insert."""
    source_df = batch_df.withColumn(
        "row_hash",
        F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]))
    )
    key_condition = " AND ".join([f"t.{k} = s.{k}" for k in business_key])

    if DeltaTable.isDeltaTable(spark, target_table):
        DeltaTable.forName(spark, target_table).alias("t") \
            .merge(source_df.alias("s"), key_condition) \
            .whenMatchedUpdate(condition="s.row_hash <> t.row_hash", set={"*": "s.*"}) \
            .whenNotMatchedInsertAll() \
            .execute()
        logger.info(f"SCD1 merge complete on {target_table}")
    else:
        source_df.write.format("delta").mode("overwrite") \
                 .option("overwriteSchema", "true").saveAsTable(target_table)
        logger.info(f"SCD1: {target_table} created (first load).")

# ---------------------------------------------------------------------------
# SCD TYPE 2 — expire + insert MERGE
# ---------------------------------------------------------------------------

def apply_scd2(
    batch_df: DataFrame,
    target_table: str,
    business_key: list,
    hash_cols: list
) -> None:
    """SCD2: expire changed active records, insert new version as active."""
    source_df = batch_df.withColumn(
        "row_hash",
        F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]))
    )

    if not DeltaTable.isDeltaTable(spark, target_table):
        source_df \
            .withColumn("eff_from",    F.current_timestamp()) \
            .withColumn("eff_to",      F.lit(HIGH_DATE).cast("timestamp")) \
            .withColumn("crnt_flag",   F.lit(True)) \
            .withColumn("inserted_at", F.current_timestamp()) \
            .write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(target_table)
        logger.info(f"SCD2: {target_table} created (first load).")
        return

    key_condition    = " AND ".join([f"t.{k} = s.{k}" for k in business_key])
    expire_condition = f"{key_condition} AND t.crnt_flag = true AND s.row_hash <> t.row_hash"

    # Step 1: Expire changed active records
    DeltaTable.forName(spark, target_table).alias("t") \
        .merge(source_df.alias("s"), expire_condition) \
        .whenMatchedUpdate(set={
            "crnt_flag": F.lit(False),
            "eff_to":    F.current_timestamp()
        }) \
        .execute()

    # Step 2: Insert new / changed records as active
    active_df = spark.table(target_table).filter(F.col("crnt_flag") == True) \
                     .select(*business_key, "row_hash").withColumnRenamed("row_hash", "_existing_hash")

    new_df = source_df \
        .join(active_df, on=business_key, how="left") \
        .filter(
            F.col("_existing_hash").isNull() |
            (F.col("row_hash") != F.col("_existing_hash"))
        ) \
        .drop("_existing_hash") \
        .withColumn("eff_from",    F.current_timestamp()) \
        .withColumn("eff_to",      F.lit(HIGH_DATE).cast("timestamp")) \
        .withColumn("crnt_flag",   F.lit(True)) \
        .withColumn("inserted_at", F.current_timestamp())

    if new_df.count() > 0:
        new_df.write.format("delta").mode("append").saveAsTable(target_table)
        logger.info(f"SCD2: {new_df.count()} records inserted into {target_table}")
    else:
        logger.info(f"SCD2: no changes for {target_table}")

# ---------------------------------------------------------------------------
# START STREAMING DOWNSTREAM JOB — one per dataset
# ---------------------------------------------------------------------------

def start_downstream_stream(config: dict, spark: SparkSession):
    """
    Start a streaming job for one dataset:
      - readStream from Multiplexer Delta table filtered by dataset_id
      - foreachBatch applies SCD1 or SCD2 to the downstream Silver table
      - DELETE operations: hard-delete (SCD1) or soft-close eff_to (SCD2)
      - Independent checkpoint per dataset enables parallel execution
    """
    dataset_id   = config["dataset_id"]
    target_table = config["target_table"]
    scd_type     = config.get("scd_type", "SCD1").upper()
    business_key = [k.strip() for k in config.get("business_key_cols", "").split(",")]
    hash_cols    = [c.strip() for c in config.get("hash_cols", "").split(",")]
    # Each dataset gets its own checkpoint path under the shared base
    checkpoint   = config["checkpoint_path"].rstrip("/") + f"/downstream/{dataset_id}/"

    logger.info(f"Starting downstream stream: {dataset_id} \u2192 {target_table} ({scd_type})")

    # Append-only readStream from Multiplexer — filter to this dataset only
    multiplexer_stream = (
        spark.readStream
        .format("delta")
        .table(MULTIPLEXER_TABLE)
        .filter(F.col("dataset_id") == dataset_id)
    )

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        if count == 0:
            return
        logger.info(f"[{dataset_id}] Batch {batch_id}: {count} records \u2192 {target_table}")

        inserts_updates = batch_df.filter(F.col("operation").isin("INSERT", "UPDATE"))
        deletes         = batch_df.filter(F.col("operation") == "DELETE")

        if inserts_updates.count() > 0:
            if scd_type == "SCD1":
                apply_scd1(inserts_updates, target_table, business_key, hash_cols)
            elif scd_type == "SCD2":
                apply_scd2(inserts_updates, target_table, business_key, hash_cols)

        # Handle DELETEs: hard-delete for SCD1, soft-close for SCD2
        if deletes.count() > 0 and DeltaTable.isDeltaTable(spark, target_table):
            key_cond = " AND ".join([f"t.{k} = d.{k}" for k in business_key])
            if scd_type == "SCD1":
                DeltaTable.forName(spark, target_table).alias("t") \
                    .merge(deletes.alias("d"), key_cond) \
                    .whenMatchedDelete() \
                    .execute()
            elif scd_type == "SCD2":
                DeltaTable.forName(spark, target_table).alias("t") \
                    .merge(deletes.alias("d"), f"{key_cond} AND t.crnt_flag = true") \
                    .whenMatchedUpdate(set={
                        "crnt_flag": F.lit(False),
                        "eff_to":    F.current_timestamp()
                    }) \
                    .execute()

    return (
        multiplexer_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="60 seconds")
        .queryName(f"downstream_{dataset_id}")
        .start()
    )

# ---------------------------------------------------------------------------
# MAIN — start all streaming downstream jobs in parallel
# ---------------------------------------------------------------------------

def main(spark: SparkSession) -> None:
    configs = load_active_configs(spark)
    queries = []

    for config in configs:
        try:
            query = start_downstream_stream(config, spark)
            queries.append({"dataset_id": config["dataset_id"], "query": query})
        except Exception as e:
            logger.error(f"Failed to start downstream stream for {config['dataset_id']}: {e}")

    logger.info(f"Downstream streaming jobs active: {len(queries)}")

    # Keep all streams running; each has its own checkpoint and SCD logic
    for q in queries:
        q["query"].awaitTermination()


if __name__ == "__main__":
    main(spark)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("scd_orchestration")
spark  = SparkSession.builder.getOrCreate()

CONFIG_TABLE = "catalog.control.dataset_config"
HIGH_DATE    = "9999-12-31T23:59:59"

# ---------------------------------------------------------------------------
# LOAD CONFIG FOR A SPECIFIC DATASET
# ---------------------------------------------------------------------------

def get_config(spark: SparkSession, dataset_id: str) -> dict:
    rows = spark.table(CONFIG_TABLE) \
                .filter(F.col("dataset_id") == dataset_id) \
                .collect()
    if not rows:
        raise ValueError(f"Dataset '{dataset_id}' not found in config table.")
    return rows[0].asDict()


# ---------------------------------------------------------------------------
# SCD TYPE 1 — latest-record-wins MERGE
# ---------------------------------------------------------------------------

def apply_scd1(
    batch_df: DataFrame,
    target_table: str,
    business_key: list,
    hash_cols: list
) -> None:
    """
    SCD1: MERGE upsert — on key match and hash change, update all columns.
    On no match, insert.
    """
    # Compute row hash
    source_df = batch_df.withColumn(
        "row_hash",
        F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]))
    )

    key_condition = " AND ".join([f"t.{k} = s.{k}" for k in business_key])

    if DeltaTable.isDeltaTable(spark, target_table):
        DeltaTable.forName(spark, target_table).alias("t") \
            .merge(source_df.alias("s"), key_condition) \
            .whenMatchedUpdate(condition="s.row_hash <> t.row_hash", set={"*": "s.*"}) \
            .whenNotMatchedInsertAll() \
            .execute()
        logger.info(f"SCD1 merge complete on {target_table}")
    else:
        source_df.write.format("delta").mode("overwrite") \
                 .option("overwriteSchema", "true") \
                 .saveAsTable(target_table)
        logger.info(f"SCD1: {target_table} created (first load).")


# ---------------------------------------------------------------------------
# SCD TYPE 2 — expire + insert MERGE
# ---------------------------------------------------------------------------

def apply_scd2(
    batch_df: DataFrame,
    target_table: str,
    business_key: list,
    hash_cols: list
) -> None:
    """
    SCD2:
    1. Expire currently active records where attributes have changed.
    2. Insert new version as the active record.
    """
    source_df = batch_df.withColumn(
        "row_hash",
        F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]))
    )

    if not DeltaTable.isDeltaTable(spark, target_table):
        # First load
        source_df \
            .withColumn("eff_from",    F.current_timestamp()) \
            .withColumn("eff_to",      F.lit(HIGH_DATE).cast("timestamp")) \
            .withColumn("crnt_flag",   F.lit(True)) \
            .withColumn("inserted_at", F.current_timestamp()) \
            .write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        logger.info(f"SCD2: {target_table} created (first load).")
        return

    key_condition   = " AND ".join([f"t.{k} = s.{k}" for k in business_key])
    expire_condition = f"{key_condition} AND t.crnt_flag = true AND s.row_hash <> t.row_hash"

    # Step 1: Expire changed records
    DeltaTable.forName(spark, target_table).alias("t") \
        .merge(source_df.alias("s"), expire_condition) \
        .whenMatchedUpdate(set={
            "crnt_flag":  F.lit(False),
            "eff_to":     F.current_timestamp()
        }) \
        .execute()

    # Step 2: Identify and insert new/changed records
    active_df = spark.table(target_table) \
                     .filter(F.col("crnt_flag") == True) \
                     .select(*business_key, "row_hash") \
                     .withColumnRenamed("row_hash", "_existing_hash")

    new_df = source_df \
        .join(active_df, on=business_key, how="left") \
        .filter(
            F.col("_existing_hash").isNull() |
            (F.col("row_hash") != F.col("_existing_hash"))
        ) \
        .drop("_existing_hash") \
        .withColumn("eff_from",    F.current_timestamp()) \
        .withColumn("eff_to",      F.lit(HIGH_DATE).cast("timestamp")) \
        .withColumn("crnt_flag",   F.lit(True)) \
        .withColumn("inserted_at", F.current_timestamp())

    new_count = new_df.count()
    if new_count > 0:
        new_df.write.format("delta").mode("append").saveAsTable(target_table)
        logger.info(f"SCD2: {new_count} new/updated records inserted into {target_table}")
    else:
        logger.info(f"SCD2: No changes for {target_table}")


# ---------------------------------------------------------------------------
# DISPATCHER — routes to SCD1 or SCD2 based on config
# ---------------------------------------------------------------------------

def run_scd_for_dataset(dataset_id: str, spark: SparkSession) -> None:
    config = get_config(spark, dataset_id)

    bronze_table  = config["bronze_table"]
    silver_table  = config["silver_table"]
    scd_type      = config.get("scd_type", "SCD1").upper()
    business_key  = config.get("business_key_cols", "").split(",")
    hash_cols     = config.get("hash_cols", "").split(",")

    logger.info(f"Processing {dataset_id}: bronze={bronze_table}, silver={silver_table}, scd={scd_type}")

    # Read from Bronze (incremental via Delta CDF or full batch)
    batch_df = spark.table(bronze_table)

    if scd_type == "SCD1":
        apply_scd1(batch_df, silver_table, business_key, hash_cols)
    elif scd_type == "SCD2":
        apply_scd2(batch_df, silver_table, business_key, hash_cols)
    else:
        raise ValueError(f"Unsupported scd_type: {scd_type} for dataset {dataset_id}")


# ---------------------------------------------------------------------------
# MAIN — process all active datasets
# ---------------------------------------------------------------------------

def main(spark: SparkSession) -> None:
    configs = spark.table(CONFIG_TABLE) \
                   .filter(F.col("is_active") == True) \
                   .select("dataset_id").collect()

    for row in configs:
        try:
            run_scd_for_dataset(row["dataset_id"], spark)
        except Exception as e:
            logger.error(f"Error processing {row['dataset_id']}: {e}")
            # Continue with remaining datasets


if __name__ == "__main__":
    main(spark)
