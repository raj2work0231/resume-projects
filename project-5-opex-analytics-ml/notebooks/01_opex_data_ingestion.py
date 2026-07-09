# =============================================================================
# Project 5 – OPEX Analytics, Geospatial Dashboards &amp; Contract Cost Analysis
# Notebook: 01_opex_data_ingestion.py
#
# Purpose : Ingest operational expenditure datasets from ADLS Gen2 into
#           Bronze Delta tables. Handles multiple OPEX asset categories
#           (power supplies, poles, cables, right-of-way).
#
# Stack   : Azure Databricks | PySpark | Delta Lake
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import logging

logger = logging.getLogger("opex_ingestion")
spark  = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION — asset categories and source paths
# ---------------------------------------------------------------------------
OPEX_SOURCES = {
    "power_supply": {
        "source_path":   "abfss://raw@<storage>.dfs.core.windows.net/opex/power-supply/",
        "bronze_table":  "catalog.bronze.opex_power_supply",
        "file_format":   "csv",
        "partition_col": "billing_period"
    },
    "poles": {
        "source_path":   "abfss://raw@<storage>.dfs.core.windows.net/opex/poles/",
        "bronze_table":  "catalog.bronze.opex_poles",
        "file_format":   "csv",
        "partition_col": "asset_year"
    },
    "underground_cable": {
        "source_path":   "abfss://raw@<storage>.dfs.core.windows.net/opex/underground-cable/",
        "bronze_table":  "catalog.bronze.opex_underground_cable",
        "file_format":   "parquet",
        "partition_col": "asset_year"
    },
    "right_of_way": {
        "source_path":   "abfss://raw@<storage>.dfs.core.windows.net/opex/row/",
        "bronze_table":  "catalog.bronze.opex_right_of_way",
        "file_format":   "csv",
        "partition_col": "year_month"
    }
}

# ---------------------------------------------------------------------------
# GENERIC OPEX INGESTION FUNCTION
# ---------------------------------------------------------------------------

def ingest_opex_category(
    spark: SparkSession,
    category: str,
    config: dict
) -> int:
    """
    Ingest one OPEX asset category from its ADLS source path to Bronze Delta.
    Returns the number of records written.
    """
    source_path   = config["source_path"]
    bronze_table  = config["bronze_table"]
    file_format   = config["file_format"]
    partition_col = config["partition_col"]

    logger.info(f"Ingesting OPEX category: {category} from {source_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .format(file_format)
        .load(source_path)
    )

    # Attach audit columns
    enriched = (
        df
        .withColumn("asset_category", F.lit(category))
        .withColumn("ingest_ts",       F.current_timestamp())
        .withColumn("ingest_date",     F.to_date(F.current_timestamp()))
        .withColumn("source_path",     F.lit(source_path))
        .withColumn("row_hash",
            F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in df.columns[:5]]))
        )
    )

    (
        enriched.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("ingest_date")
        .saveAsTable(bronze_table)
    )

    count = enriched.count()
    logger.info(f"  Written {count:,} records to {bronze_table}")
    return count

# ---------------------------------------------------------------------------
# UNIFIED OPEX BRONZE VIEW
# ---------------------------------------------------------------------------

def create_unified_opex_view(spark: SparkSession) -> None:
    """
    Create a unified view across all OPEX Bronze tables for cross-category analysis.
    Each category table must have: asset_id, latitude, longitude, cost_amount, billing_period.
    """
    unions = []
    for category, config in OPEX_SOURCES.items():
        table = config["bronze_table"]
        try:
            unions.append(f"""
                SELECT
                    asset_id,
                    '{category}' AS asset_category,
                    latitude,
                    longitude,
                    cost_amount,
                    supplier_id,
                    billing_period,
                    region,
                    ingest_date
                FROM {table}
            """)
        except Exception as e:
            logger.warning(f"Could not include {table}: {e}")

    if unions:
        spark.sql(f"""
            CREATE OR REPLACE VIEW catalog.bronze.v_opex_all AS
            {' UNION ALL '.join(unions)}
        """)
        logger.info("Unified OPEX view created: catalog.bronze.v_opex_all")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main(spark: SparkSession) -> None:
    total = 0
    for category, config in OPEX_SOURCES.items():
        try:
            count = ingest_opex_category(spark, category, config)
            total += count
        except Exception as e:
            logger.error(f"Failed to ingest {category}: {e}")

    create_unified_opex_view(spark)
    logger.info(f"OPEX ingestion complete. Total records: {total:,}")


if __name__ == "__main__":
    main(spark)
