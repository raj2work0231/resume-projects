# =============================================================================
# Project 3 – Metadata-Driven AutoLoader Framework
# Notebook: 02_schema_evolution_handler.py
#
# Purpose : Handle schema evolution scenarios arising from upstream changes.
#           Demonstrates addNewColumns, type migration, and backward-compatible
#           schema versioning with audit logging.
#
# Stack   : Azure Databricks | PySpark | Delta Lake | AutoLoader
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from delta.tables import DeltaTable
import logging, json
from datetime import datetime, timezone

logger = logging.getLogger("schema_evolution")
spark  = SparkSession.builder.getOrCreate()

SCHEMA_LOG_TABLE = "catalog.control.schema_version_log"

# ---------------------------------------------------------------------------
# 1. DETECT SCHEMA DRIFT between a new batch and the existing Delta table
# ---------------------------------------------------------------------------

def detect_schema_drift(new_df: DataFrame, table_name: str) -> dict:
    """
    Compare the schema of an incoming batch against the registered Delta table.
    Returns a dict with: new_columns, dropped_columns, type_changes.
    """
    existing_schema = spark.table(table_name).schema
    new_schema      = new_df.schema

    existing_fields = {f.name: f.dataType for f in existing_schema.fields}
    new_fields      = {f.name: f.dataType for f in new_schema.fields}

    new_cols     = [c for c in new_fields if c not in existing_fields]
    dropped_cols = [c for c in existing_fields if c not in new_fields]
    type_changes = {
        c: {"old": str(existing_fields[c]), "new": str(new_fields[c])}
        for c in new_fields
        if c in existing_fields and str(existing_fields[c]) != str(new_fields[c])
    }

    drift = {
        "table":        table_name,
        "new_columns":  new_cols,
        "dropped_cols": dropped_cols,
        "type_changes": type_changes,
        "has_drift":    bool(new_cols or dropped_cols or type_changes)
    }

    if drift["has_drift"]:
        logger.warning(f"Schema drift detected on {table_name}: {drift}")
    else:
        logger.info(f"No schema drift on {table_name}")

    return drift

# ---------------------------------------------------------------------------
# 2. LOG SCHEMA CHANGES to audit Delta table
# ---------------------------------------------------------------------------

def log_schema_version(table_name: str, dataset_id: str, drift: dict) -> None:
    """Persist schema change events for audit and lineage tracking."""
    entry = [{
        "table_name":    table_name,
        "dataset_id":    dataset_id,
        "new_columns":   json.dumps(drift.get("new_columns", [])),
        "dropped_cols":  json.dumps(drift.get("dropped_cols", [])),
        "type_changes":  json.dumps(drift.get("type_changes", {})),
        "detected_ts":   datetime.now(timezone.utc).isoformat()
    }]
    spark.createDataFrame(entry).write \
         .format("delta").mode("append") \
         .saveAsTable(SCHEMA_LOG_TABLE)
    logger.info(f"Schema version logged for {table_name}")

# ---------------------------------------------------------------------------
# 3. APPLY SCHEMA EVOLUTION — safe handling strategies
# ---------------------------------------------------------------------------

def apply_schema_evolution(df: DataFrame, target_table: str, mode: str = "merge") -> DataFrame:
    """
    Handle schema evolution before writing to Delta.

    Modes:
      merge  → add new columns to the Delta schema (mergeSchema=true)
      cast   → attempt safe type casting of changed columns
      drop   → drop columns not in the existing schema (strict backward compat)
    """
    if mode == "drop":
        # Strict mode: only keep columns already in the target schema
        existing_cols = [f.name for f in spark.table(target_table).schema.fields]
        df = df.select([c for c in df.columns if c in existing_cols])
        logger.info(f"Drop mode: retained {len(existing_cols)} columns.")

    elif mode == "cast":
        # Cast columns that have changed type to match target schema
        existing_schema = spark.table(target_table).schema
        target_fields   = {f.name: f.dataType for f in existing_schema.fields}
        for col_name, target_type in target_fields.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(target_type))
        logger.info("Cast mode: type alignment applied.")

    # mode == "merge": no transformation needed — mergeSchema handles it at write time
    return df

# ---------------------------------------------------------------------------
# 4. SAFE WRITE WITH SCHEMA EVOLUTION
# ---------------------------------------------------------------------------

def write_with_schema_evolution(
    df: DataFrame,
    target_table: str,
    partition_cols: list = None,
    write_mode: str = "append"
) -> None:
    """
    Write DataFrame to Delta table with mergeSchema enabled.
    New columns are automatically added to the Delta table schema.
    """
    writer = df.write.format("delta").mode(write_mode).option("mergeSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(target_table)
    logger.info(f"Written to {target_table} with schema evolution (mergeSchema=true)")

# ---------------------------------------------------------------------------
# 5. RESCUE DATA — capture unparseable rows in _rescued_data column
# ---------------------------------------------------------------------------

def enable_rescue_data(config: dict) -> dict:
    """
    Add the rescue data option to AutoLoader config.
    AutoLoader stores rows that couldn't be parsed in the _rescued_data column
    instead of failing the stream.
    """
    config["cloudFiles.rescuedDataColumn"] = "_rescued_data"
    config["cloudFiles.schemaEvolutionMode"] = "rescue"
    return config


def write_rescued_data(df: DataFrame, rescue_table: str) -> None:
    """Persist rows with non-null _rescued_data to a quarantine table."""
    rescued = df.filter(F.col("_rescued_data").isNotNull())
    if rescued.count() > 0:
        rescued.write.format("delta").mode("append").saveAsTable(rescue_table)
        logger.warning(f"{rescued.count()} rows rescued and written to {rescue_table}")

# ---------------------------------------------------------------------------
# DEMO — show evolution handling in action
# ---------------------------------------------------------------------------

def demonstrate_schema_evolution(spark: SparkSession) -> None:
    """
    Illustrative demo:
    1. Create a Delta table with schema V1
    2. Introduce new columns (V2 batch)
    3. Detect drift, log, and merge schema
    """
    table = "catalog.demo.schema_evolution_test"

    # V1 data
    v1 = spark.createDataFrame(
        [("A001", "John", 100.0), ("A002", "Jane", 200.0)],
        ["account_id", "name", "amount"]
    )
    v1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    logger.info("V1 schema created.")

    # V2 data — new columns: region, channel
    v2 = spark.createDataFrame(
        [("A003", "Bob", 300.0, "CA", "ONLINE"), ("A004", "Alice", 400.0, "NY", "MOBILE")],
        ["account_id", "name", "amount", "region", "channel"]
    )

    drift = detect_schema_drift(v2, table)
    if drift["has_drift"]:
        log_schema_version(table, "demo_dataset", drift)
        write_with_schema_evolution(v2, table, write_mode="append")
        logger.info("V2 schema merged successfully.")

    spark.table(table).printSchema()


# Uncomment to run demo:
# demonstrate_schema_evolution(spark)
