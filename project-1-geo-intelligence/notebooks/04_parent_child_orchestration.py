# =============================================================================
# Project 1 – Geo-Location Intelligence Platform
# Notebook: 04_parent_child_orchestration.py
#
# Purpose : Parent notebook that reads a campaign configuration table and
#           dynamically submits child notebooks for each active campaign
#           using concurrent threading. Tracks execution status in a Delta
#           control table with retry logic via tenacity.
#
# Stack   : Azure Databricks | Databricks Workflows | dbutils | tenacity
# =============================================================================

# COMMAND ----------
# %pip install tenacity

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from datetime import datetime, timezone
import logging
import json

logger = logging.getLogger("campaign_orchestration")
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
CAMPAIGN_CONFIG_TABLE = "catalog.control.campaign_config"
EXECUTION_STATUS_TABLE = "catalog.control.campaign_execution_status"
CHILD_NOTEBOOK_PATH   = "/Repos/data-engineering/geo-intelligence/notebooks/child_campaign_processor"
MAX_PARALLEL_CAMPAIGNS = 5          # max concurrent child jobs
CHILD_TIMEOUT_SECONDS  = 3600       # 1 hour timeout per child
MAX_RETRIES            = 3
RETRY_WAIT_MIN         = 30         # seconds
RETRY_WAIT_MAX         = 120        # seconds

# ---------------------------------------------------------------------------
# STEP 1: Load active campaign configuration
# ---------------------------------------------------------------------------

def load_active_campaigns(spark: SparkSession) -> list[dict]:
    """
    Read active campaigns from control Delta table.
    Returns a list of campaign config dicts.
    """
    df = (
        spark.table(CAMPAIGN_CONFIG_TABLE)
        .filter(F.col("is_active") == True)
        .filter(F.col("run_date") == F.current_date())
        .select(
            "campaign_id", "campaign_name", "geo_radius_km",
            "h3_resolution", "target_segments", "start_date", "end_date"
        )
    )
    campaigns = [row.asDict() for row in df.collect()]
    logger.info(f"Loaded {len(campaigns)} active campaigns for today.")
    return campaigns

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 2: Write execution status to Delta control table
# ---------------------------------------------------------------------------

def upsert_execution_status(
    campaign_id: str,
    status: str,           # SUBMITTED | RUNNING | SUCCESS | FAILED
    error_msg: str = None,
    run_ts: datetime = None
) -> None:
    """Upsert campaign execution status into the Delta control table."""
    if run_ts is None:
        run_ts = datetime.now(timezone.utc)

    status_data = [{
        "campaign_id":   campaign_id,
        "status":        status,
        "error_message": error_msg or "",
        "run_ts":        run_ts.isoformat(),
        "updated_at":    datetime.now(timezone.utc).isoformat()
    }]
    status_df = spark.createDataFrame(status_data)

    if DeltaTable.isDeltaTable(spark, EXECUTION_STATUS_TABLE):
        DeltaTable.forName(spark, EXECUTION_STATUS_TABLE).alias("t") \
            .merge(status_df.alias("s"), "t.campaign_id = s.campaign_id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        status_df.write.format("delta").mode("append").saveAsTable(EXECUTION_STATUS_TABLE)

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 3: Child notebook runner with retry logic
# ---------------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
    reraise=True
)
def run_child_notebook(campaign: dict) -> dict:
    """
    Run the child campaign notebook via dbutils.notebook.run().
    Retries up to MAX_RETRIES times with exponential back-off.
    Returns result dict with status.
    """
    campaign_id = campaign["campaign_id"]
    params = {
        "campaign_id":    str(campaign_id),
        "campaign_name":  campaign["campaign_name"],
        "geo_radius_km":  str(campaign["geo_radius_km"]),
        "h3_resolution":  str(campaign["h3_resolution"]),
        "start_date":     str(campaign["start_date"]),
        "end_date":       str(campaign["end_date"]),
    }

    try:
        upsert_execution_status(campaign_id, "RUNNING")
        result = dbutils.notebook.run(
            CHILD_NOTEBOOK_PATH,
            timeout_seconds=CHILD_TIMEOUT_SECONDS,
            arguments=params
        )
        result_data = json.loads(result) if result else {}
        upsert_execution_status(campaign_id, "SUCCESS")
        logger.info(f"Campaign {campaign_id} SUCCEEDED.")
        return {"campaign_id": campaign_id, "status": "SUCCESS", "result": result_data}

    except Exception as e:
        error_msg = str(e)
        logger.warning(f"Campaign {campaign_id} attempt failed: {error_msg}")
        upsert_execution_status(campaign_id, "FAILED", error_msg=error_msg)
        raise   # tenacity will retry

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 4: Concurrent campaign execution
# ---------------------------------------------------------------------------

def run_campaigns_concurrently(campaigns: list[dict]) -> dict:
    """
    Submit all campaigns to a thread pool for concurrent execution.
    Collects results; logs summary.
    Returns dict with counts: success, failed.
    """
    results = {"success": 0, "failed": 0, "errors": []}

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_CAMPAIGNS) as executor:
        future_to_campaign = {
            executor.submit(run_child_notebook, campaign): campaign["campaign_id"]
            for campaign in campaigns
        }
        upsert_execution_status(c["campaign_id"], "SUBMITTED") if True else None
        for campaign in campaigns:
            upsert_execution_status(campaign["campaign_id"], "SUBMITTED")

        for future in as_completed(future_to_campaign):
            cid = future_to_campaign[future]
            try:
                result = future.result()
                if result["status"] == "SUCCESS":
                    results["success"] += 1
                else:
                    results["failed"] += 1
            except RetryError as re:
                logger.error(f"Campaign {cid} FAILED after {MAX_RETRIES} retries: {re}")
                results["failed"] += 1
                results["errors"].append({"campaign_id": cid, "error": str(re)})
            except Exception as ex:
                logger.error(f"Unexpected error for campaign {cid}: {ex}")
                results["failed"] += 1
                results["errors"].append({"campaign_id": cid, "error": str(ex)})

    return results

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 5: Summary report
# ---------------------------------------------------------------------------

def generate_summary_report(results: dict, campaigns: list[dict]) -> None:
    """Print and log execution summary. In production, write to monitoring table."""
    total = len(campaigns)
    print("=" * 60)
    print("CAMPAIGN ORCHESTRATION SUMMARY")
    print("=" * 60)
    print(f"  Total campaigns   : {total}")
    print(f"  Succeeded         : {results['success']}")
    print(f"  Failed            : {results['failed']}")
    if results["errors"]:
        print("  Failed campaigns  :")
        for err in results["errors"]:
            print(f"    - {err['campaign_id']}: {err['error'][:80]}")
    print("=" * 60)

    # Write summary to control table
    summary_data = [{
        "run_ts":    datetime.now(timezone.utc).isoformat(),
        "total":     total,
        "succeeded": results["success"],
        "failed":    results["failed"],
        "errors":    json.dumps(results["errors"])
    }]
    spark.createDataFrame(summary_data) \
         .write.format("delta").mode("append") \
         .saveAsTable("catalog.control.orchestration_run_summary")

# COMMAND ----------
# ---------------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------------

def main():
    logger.info("Parent orchestration notebook started.")
    campaigns = load_active_campaigns(spark)

    if not campaigns:
        logger.warning("No active campaigns found for today. Exiting.")
        dbutils.notebook.exit(json.dumps({"status": "NO_CAMPAIGNS"}))
        return

    logger.info(f"Running {len(campaigns)} campaigns with max {MAX_PARALLEL_CAMPAIGNS} in parallel …")
    results = run_campaigns_concurrently(campaigns)
    generate_summary_report(results, campaigns)

    exit_status = "SUCCESS" if results["failed"] == 0 else "PARTIAL_FAILURE"
    logger.info(f"Orchestration complete: {exit_status}")
    dbutils.notebook.exit(json.dumps({
        "status": exit_status,
        "success": results["success"],
        "failed":  results["failed"]
    }))


main()
