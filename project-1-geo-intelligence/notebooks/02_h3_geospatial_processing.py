# =============================================================================
# Project 1 – Geo-Location Intelligence Platform
# Notebook: 02_h3_geospatial_processing.py
#
# Purpose : Apply Uber H3 hexagonal indexing (resolution 11), Voronoi / 
#           sub-Voronoi cell-site polygon mapping, POI enrichment, and 
#           OpenStreetMap perimeter generation to Silver-layer network events.
#
# Stack   : Azure Databricks | PySpark | Uber H3 | GeoPandas | Shapely
# =============================================================================

# COMMAND ----------
# %pip install h3 shapely geopandas osmnx

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DoubleType
import h3
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.ops import unary_union
import pandas as pd
import logging

logger = logging.getLogger("h3_geospatial")
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
SILVER_TABLE     = "catalog.silver.network_events_clean"
CELL_SITES_TABLE = "catalog.reference.cell_sites"
POI_TABLE        = "catalog.reference.poi_data"
GOLD_TABLE       = "catalog.gold.geospatial_adcampaign_segments"
H3_RESOLUTION    = 11          # ~25m edge length — fine-grain for location
SEARCH_RADIUS_KM = 0.5         # POI enrichment radius

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 1 — Assign H3 Hex Index to each event
# ---------------------------------------------------------------------------

@F.udf(returnType=StringType())
def lat_lon_to_h3(lat: float, lon: float, resolution: int = H3_RESOLUTION) -> str:
    """Convert lat/lon to H3 hexagon index at the specified resolution."""
    if lat is None or lon is None:
        return None
    try:
        return h3.latlng_to_cell(lat, lon, resolution)
    except Exception:
        return None


@F.udf(returnType=ArrayType(StringType()))
def get_h3_neighbours(h3_index: str, k_rings: int = 1) -> list:
    """Return k-ring neighbours of a hex cell (for spatial smoothing)."""
    if h3_index is None:
        return []
    return list(h3.grid_disk(h3_index, k_rings))


def assign_h3_index(df: DataFrame) -> DataFrame:
    """Enrich a DataFrame with H3 index and neighbour cells."""
    return (
        df
        .withColumn("h3_index",      lat_lon_to_h3(F.col("latitude"), F.col("longitude")))
        .withColumn("h3_neighbours", get_h3_neighbours(F.col("h3_index")))
        .withColumn("h3_resolution", F.lit(H3_RESOLUTION))
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 2 — Voronoi Cell-Site Mapping
# Assign each event to the nearest cell site using Voronoi partitioning.
# In production this is pre-computed and stored as a reference Delta table.
# ---------------------------------------------------------------------------

def load_voronoi_reference(spark: SparkSession) -> DataFrame:
    """
    Load pre-computed Voronoi polygon reference table.
    Each row contains: cell_site_id, voronoi_polygon (WKT), sub_voronoi_id
    """
    return spark.table(CELL_SITES_TABLE).select(
        "cell_site_id", "latitude", "longitude",
        "voronoi_polygon_wkt", "sub_voronoi_polygon_wkt",
        "site_type", "technology_band"
    )


@F.udf(returnType=StringType())
def point_in_voronoi(lat: float, lon: float, polygons_json: str) -> str:
    """
    Determine which Voronoi cell contains the point.
    polygons_json: JSON string of {cell_site_id: wkt_polygon} mapping.
    """
    import json
    from shapely.geometry import Point
    from shapely.wkt import loads as wkt_loads

    if lat is None or lon is None:
        return None
    pt = Point(lon, lat)
    try:
        polygons = json.loads(polygons_json)
        for site_id, wkt in polygons.items():
            poly = wkt_loads(wkt)
            if poly.contains(pt):
                return site_id
    except Exception:
        pass
    return None


def join_voronoi_cells(events_df: DataFrame, voronoi_df: DataFrame) -> DataFrame:
    """
    Broadcast-join Voronoi reference to assign cell_site_id to each event.
    For large reference tables prefer H3 spatial join approach below.
    """
    # Efficient approach: join on H3 index pre-assigned to each Voronoi polygon
    return events_df.join(
        F.broadcast(voronoi_df.select(
            "cell_site_id", "h3_index", "site_type", "technology_band"
        ).withColumnRenamed("h3_index", "site_h3_index")),
        on=events_df["h3_index"] == voronoi_df["site_h3_index"],
        how="left"
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 3 — POI (Point of Interest) Enrichment
# ---------------------------------------------------------------------------

def enrich_with_poi(events_df: DataFrame, poi_df: DataFrame) -> DataFrame:
    """
    Join POI data to events based on H3 index proximity.
    POI categories: retail, transit, residential, commercial, etc.
    """
    # Explode neighbours to find POIs within k-ring distance
    events_with_rings = events_df.withColumn(
        "h3_search_cells",
        F.array_union(F.array(F.col("h3_index")), F.col("h3_neighbours"))
    ).withColumn("search_cell", F.explode(F.col("h3_search_cells")))

    # Join POI by H3 cell
    poi_enriched = events_with_rings.join(
        poi_df.withColumnRenamed("h3_index", "poi_h3"),
        on=events_with_rings["search_cell"] == poi_df["poi_h3"],
        how="left"
    )

    # Aggregate: top POI category per event
    return (
        poi_enriched
        .groupBy("event_id", "device_id", "h3_index", "cell_site_id",
                 "latitude", "longitude", "event_ts", "ingest_date")
        .agg(
            F.collect_set("poi_category").alias("nearby_poi_categories"),
            F.count("poi_id").alias("nearby_poi_count"),
            F.first("poi_category").alias("primary_poi_category")
        )
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 4 — OSM Perimeter / Area Tagging
# ---------------------------------------------------------------------------

def tag_osm_area(df: DataFrame) -> DataFrame:
    """
    Tag each H3 hex with the OSM area type (urban / suburban / rural).
    In production: pre-computed OSM area lookup table joined by H3 index.
    """
    # Illustrative static rule — replace with actual OSM lookup join
    return df.withColumn(
        "osm_area_type",
        F.when(F.col("nearby_poi_count") > 20, F.lit("urban"))
         .when(F.col("nearby_poi_count") > 5,  F.lit("suburban"))
         .otherwise(F.lit("rural"))
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 5 — Advertisement Campaign Segment Assignment
# ---------------------------------------------------------------------------

SEGMENT_RULES = {
    "retail_visitor":    "array_contains(nearby_poi_categories, 'retail')",
    "transit_commuter":  "array_contains(nearby_poi_categories, 'transit')",
    "home_location":     "osm_area_type = 'residential'",
    "work_location":     "osm_area_type = 'commercial'",
}


def assign_adcampaign_segments(df: DataFrame) -> DataFrame:
    """
    Apply campaign segment labelling rules; a device can belong to multiple segments.
    """
    return (
        df
        .withColumn("segment_retail",   F.array_contains(F.col("nearby_poi_categories"), "retail"))
        .withColumn("segment_transit",  F.array_contains(F.col("nearby_poi_categories"), "transit"))
        .withColumn("segment_urban",    F.col("osm_area_type") == "urban")
        .withColumn("segment_suburban", F.col("osm_area_type") == "suburban")
        .withColumn("segments",
            F.array_compact(F.array(
                F.when(F.col("segment_retail"),   F.lit("retail_visitor")),
                F.when(F.col("segment_transit"),  F.lit("transit_commuter")),
                F.when(F.col("segment_urban"),    F.lit("urban_dweller")),
                F.when(F.col("segment_suburban"), F.lit("suburban_dweller")),
            ))
        )
        .withColumn("row_hash",
            F.md5(F.concat_ws("||",
                F.col("device_id"), F.col("h3_index"),
                F.array_join(F.sort_array(F.col("segments")), ",")
            ))
        )
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def run_geospatial_pipeline(spark: SparkSession) -> None:
    logger.info("Loading Silver events …")
    events_df = spark.table(SILVER_TABLE)

    logger.info("Assigning H3 index …")
    events_df = assign_h3_index(events_df)

    logger.info("Joining Voronoi cell sites …")
    voronoi_df = load_voronoi_reference(spark)
    events_df  = join_voronoi_cells(events_df, voronoi_df)

    logger.info("Enriching with POI data …")
    poi_df    = spark.table(POI_TABLE)
    events_df = enrich_with_poi(events_df, poi_df)

    logger.info("Tagging OSM area type …")
    events_df = tag_osm_area(events_df)

    logger.info("Assigning advertisement campaign segments …")
    events_df = assign_adcampaign_segments(events_df)

    logger.info(f"Writing to Gold table: {GOLD_TABLE}")
    (
        events_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
        .option("mergeSchema", "true")
        .partitionBy("ingest_date")
        .saveAsTable(GOLD_TABLE)
    )
    logger.info("Geospatial pipeline complete.")


if __name__ == "__main__":
    run_geospatial_pipeline(spark)
