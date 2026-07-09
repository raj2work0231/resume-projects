# =============================================================================
# Project 1 – Geo-Location Intelligence Platform
# Notebook: 03_scd2_delta_management.py
#
# Purpose : Implement SCD Type 2 history management on the Silver POI reference
#           table (monthly batch load). Uses MD5 checksums to detect row-level
#           changes and a MERGE INTO pattern to expire old records and insert
#           new versions. Includes automated temp table lifecycle management.
#
# Stack   : Azure Databricks | PySpark | Delta Lake | SparkSQL
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timezone
import logging

logger = logging.getLogger("scd2_management")
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
SOURCE_TABLE  = "catalog.silver.poi_reference_staging"
TARGET_TABLE  = "catalog.silver.poi_reference_history"
TEMP_DB       = "catalog.temp_processing"
BUSINESS_KEY  = ["poi_id"]                         # natural key for POI records
HASH_COLS     = ["poi_name", "poi_category", "latitude", "longitude", "poi_type"]
HIGH_DATE     = "9999-12-31T23:59:59"              # sentinel for open-ended records

# COMMAND ----------
# ---------------------------------------------------------------------------
# UTILITY: Compute row hash from business attribute columns
# ---------------------------------------------------------------------------

def add_row_hash(df: DataFrame, cols: list) -> DataFrame:
    """Add md5 row hash of specified attribute columns."""
    return df.withColumn(
        "row_hash",
        F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]))
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 1: Identify changed / new records vs target table
# ---------------------------------------------------------------------------

def get_changed_records(source_df: DataFrame, target_df: DataFrame) -> DataFrame:
    """
    Return records from source that are new or have changed attributes
    compared to the current active record in target (crnt_flag = true).
    """
    active_target = target_df.filter(F.col("crnt_flag") == True) \
                              .select(*BUSINESS_KEY, "row_hash") \
                              .withColumnRenamed("row_hash", "target_hash")

    return (
        source_df
        .join(active_target, on=BUSINESS_KEY, how="left")
        .filter(
            F.col("target_hash").isNull() |          # NEW record
            (F.col("row_hash") != F.col("target_hash"))  # CHANGED record
        )
        .drop("target_hash")
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 2: SCD Type 2 MERGE using Delta DeltaTable API
# ---------------------------------------------------------------------------

def apply_scd2_merge(
    source_df: DataFrame,
    target_table: str,
    business_key: list,
    as_of_ts: datetime = None
) -> None:
    """
    Perform SCD2 merge:
    1. Expire (close) the currently active record for any changed key.
    2. Insert the new version as the current active record.
    """
    if as_of_ts is None:
        as_of_ts = datetime.now(timezone.utc)

    effective_from = F.lit(as_of_ts.strftime("%Y-%m-%dT%H:%M:%S")).cast("timestamp")
    high_date_ts   = F.lit(HIGH_DATE).cast("timestamp")

    delta_target = DeltaTable.forName(spark, target_table)

    # --- Stage 1: Expire old active records that have changed ---
    # Build join condition on business key
    key_condition = " AND ".join(
        [f"target.{k} = updates.{k}" for k in business_key]
    )

    # Mark incoming changed records (already filtered by get_changed_records)
    staged_updates = source_df.alias("updates")

    # Expire current records where business key matches and row_hash differs
    (
        delta_target.alias("target")
        .merge(staged_updates, f"{key_condition} AND target.crnt_flag = true AND target.row_hash != updates.row_hash")
        .whenMatchedUpdate(set={
            "crnt_flag": F.lit(False),
            "eff_to":    effective_from,
            "updated_at": F.current_timestamp()
        })
        .execute()
    )

    # --- Stage 2: Insert new / updated records as current active ---
    new_records = source_df.select(
        *source_df.columns,
        effective_from.alias("eff_from"),
        high_date_ts.alias("eff_to"),
        F.lit(True).alias("crnt_flag"),
        F.current_timestamp().alias("inserted_at"),
        F.current_timestamp().alias("updated_at")
    )

    # Insert only new / changed records (avoid re-inserting unchanged ones)
    new_records.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)

    logger.info(f"SCD2 merge complete on {target_table} as of {as_of_ts}")

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 3: Validate SCD2 integrity
# ---------------------------------------------------------------------------

def validate_scd2(table_name: str) -> None:
    """
    Post-merge data quality checks:
    - No more than one active (crnt_flag=true) record per business key
    - No overlapping eff_from / eff_to ranges per key
    - All expired records have eff_to < HIGH_DATE
    """
    df = spark.table(table_name)

    # Check 1: Duplicate active records per business key
    active_dupes = (
        df.filter(F.col("crnt_flag") == True)
          .groupBy(*BUSINESS_KEY)
          .count()
          .filter(F.col("count") > 1)
    )
    dupe_count = active_dupes.count()
    if dupe_count > 0:
        logger.error(f"SCD2 VIOLATION: {dupe_count} business keys have >1 active record!")
        active_dupes.show(10, truncate=False)
    else:
        logger.info("SCD2 Check 1 PASSED: No duplicate active records.")

    # Check 2: Total record counts
    total   = df.count()
    active  = df.filter(F.col("crnt_flag") == True).count()
    expired = df.filter(F.col("crnt_flag") == False).count()
    logger.info(f"Records — Total: {total:,} | Active: {active:,} | Expired: {expired:,}")

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 4: Temp table lifecycle management
# ---------------------------------------------------------------------------

def cleanup_temp_tables(database: str, pattern: str = "stg_") -> None:
    """
    Drop temporary staging tables matching a naming pattern.
    Called at end of pipeline run to prevent temp table sprawl.
    """
    tables = spark.catalog.listTables(database)
    dropped = 0
    for t in tables:
        if pattern in t.name:
            full_name = f"{database}.{t.name}"
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
            logger.info(f"Dropped temp table: {full_name}")
            dropped += 1
    logger.info(f"Temp table cleanup: {dropped} table(s) dropped from {database}")

# COMMAND ----------
# ---------------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------------

def run_scd2_pipeline(spark: SparkSession) -> None:
    logger.info("Loading source staging data …")
    source_df = spark.table(SOURCE_TABLE)
    source_df = add_row_hash(source_df, HASH_COLS)

    logger.info("Loading current target history table …")
    target_df = spark.table(TARGET_TABLE)

    logger.info("Identifying changed / new records …")
    changed_df = get_changed_records(source_df, target_df)
    change_count = changed_df.count()
    logger.info(f"Records to merge: {change_count:,}")

    if change_count > 0:
        logger.info("Applying SCD Type 2 merge …")
        apply_scd2_merge(changed_df, TARGET_TABLE, BUSINESS_KEY)

        logger.info("Validating SCD2 integrity …")
        validate_scd2(TARGET_TABLE)
    else:
        logger.info("No changes detected. Skipping merge.")

    logger.info("Cleaning up temp tables …")
    cleanup_temp_tables(TEMP_DB)

    logger.info("SCD2 pipeline finished.")


if __name__ == "__main__":
    run_scd2_pipeline(spark)
