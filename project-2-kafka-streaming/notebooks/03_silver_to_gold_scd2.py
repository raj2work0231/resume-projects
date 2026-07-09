# =============================================================================
# Project 2 – Real-Time Kafka Streaming Pipeline
# Notebook: 03_multiplexer_to_downstream.py
#
# Purpose : Scheduled batch job that reads unprocessed records from the
#           Multiplexer Delta table and routes them to downstream Delta tables
#           using SCD Type 1 or SCD Type 2 MERGE, based on the scd_type defined
#           in the topic routing config. Runs on a fixed schedule (e.g. every
#           30 minutes) to control compute cost.
#
# Stack   : Azure Databricks | PySpark | Delta Lake | SCD Type 1 / SCD Type 2
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone
import logging

logger = logging.getLogger("multiplexer_router")
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
MULTIPLEXER_TABLE    = "catalog.bronze.kafka_multiplexer"
ROUTING_CONFIG_TABLE = "catalog.config.topic_routing"
HIGH_DATE            = "9999-12-31T23:59:59"

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 1 — Load unprocessed Multiplexer records
# ---------------------------------------------------------------------------

def load_unprocessed_records(spark: SparkSession) -> DataFrame:
    """
    Read records from the Multiplexer table where processed_flag = False.
    Returns all pending records for this scheduling run.
    """
    return spark.table(MULTIPLEXER_TABLE).filter(F.col("processed_flag") == False)


def load_routing_config(spark: SparkSession) -> dict:
    """
    Load topic_routing config as a Python dict keyed by target_table.
    Returns: { target_table: {scd_type, business_key_cols}, ... }
    """
    rows = spark.table(ROUTING_CONFIG_TABLE) \
                .filter(F.col("is_active") == True) \
                .select("target_table", "scd_type", "business_key_cols") \
                .collect()
    return {r["target_table"]: {"scd_type": r["scd_type"], "business_key_cols": r["business_key_cols"]} for r in rows}

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 2 — SCD Type 1 MERGE (latest-wins)
# ---------------------------------------------------------------------------

def apply_scd1_merge(source_df: DataFrame, target_table: str, business_key_cols: list) -> None:
    """
    SCD Type 1: On match (same business key) update all columns if row_hash changed.
    On no match: insert as new record.
    DELETE operations remove the active record.
    """
    key_condition = " AND ".join([f"t.{k} = s.{k}" for k in business_key_cols])

    inserts_updates = source_df.filter(F.col("operation").isin("INSERT", "UPDATE"))
    deletes         = source_df.filter(F.col("operation") == "DELETE")

    if DeltaTable.isDeltaTable(spark, target_table):
        dt = DeltaTable.forName(spark, target_table)
        (
            dt.alias("t")
            .merge(inserts_updates.alias("s"), key_condition)
            .whenMatchedUpdate(
                condition="s.row_hash <> t.row_hash",
                set={"*": "s.*"}
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        # Hard-delete for DELETE operations
        if deletes.count() > 0:
            key_filter = " AND ".join([f"t.{k} = d.{k}" for k in business_key_cols])
            (
                dt.alias("t")
                .merge(deletes.alias("d"), key_filter)
                .whenMatchedDelete()
                .execute()
            )
    else:
        inserts_updates.write.format("delta").mode("overwrite") \
                       .saveAsTable(target_table)

    logger.info(f"SCD1 merge complete on {target_table}: {inserts_updates.count()} upserts, {deletes.count()} deletes.")

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 3 — SCD Type 2 MERGE (full audit history)
# ---------------------------------------------------------------------------

def apply_scd2_merge(source_df: DataFrame, target_table: str, business_key_cols: list) -> None:
    """
    SCD Type 2: Expire old active records where attributes changed, then insert
    new version as current. DELETE operations close the active record (eff_to = now).
    """
    as_of_ts     = datetime.now(timezone.utc)
    eff_from_ts  = F.lit(as_of_ts.strftime("%Y-%m-%dT%H:%M:%S")).cast("timestamp")
    high_date_ts = F.lit(HIGH_DATE).cast("timestamp")
    key_condition = " AND ".join([f"t.{k} = s.{k}" for k in business_key_cols])

    inserts_updates = source_df.filter(F.col("operation").isin("INSERT", "UPDATE"))
    deletes         = source_df.filter(F.col("operation") == "DELETE")

    if DeltaTable.isDeltaTable(spark, target_table):
        dt = DeltaTable.forName(spark, target_table)

        # Stage 1: Expire changed active records
        (
            dt.alias("t")
            .merge(
                inserts_updates.alias("s"),
                f"{key_condition} AND t.crnt_flag = true AND t.row_hash != s.row_hash"
            )
            .whenMatchedUpdate(set={
                "crnt_flag":  F.lit(False),
                "eff_to":     eff_from_ts,
                "updated_at": F.current_timestamp()
            })
            .execute()
        )

        # Stage 2: Insert new versions (new keys + changed records)
        existing_active = spark.table(target_table) \
                               .filter(F.col("crnt_flag") == True) \
                               .select(*business_key_cols, "row_hash") \
                               .withColumnRenamed("row_hash", "existing_hash")

        new_records = (
            inserts_updates
            .join(existing_active, on=business_key_cols, how="left")
            .filter(
                F.col("existing_hash").isNull() |
                (F.col("row_hash") != F.col("existing_hash"))
            )
            .drop("existing_hash")
            .withColumn("eff_from",    eff_from_ts)
            .withColumn("eff_to",      high_date_ts)
            .withColumn("crnt_flag",   F.lit(True))
            .withColumn("inserted_at", F.current_timestamp())
            .withColumn("updated_at",  F.current_timestamp())
        )
        if new_records.count() > 0:
            new_records.write.format("delta").mode("append").saveAsTable(target_table)

        # Stage 3: Close active record for DELETEs
        if deletes.count() > 0:
            (
                dt.alias("t")
                .merge(
                    deletes.alias("s"),
                    f"{key_condition} AND t.crnt_flag = true"
                )
                .whenMatchedUpdate(set={
                    "crnt_flag":  F.lit(False),
                    "eff_to":     eff_from_ts,
                    "updated_at": F.current_timestamp()
                })
                .execute()
            )
    else:
        inserts_updates \
            .withColumn("eff_from",    eff_from_ts) \
            .withColumn("eff_to",      high_date_ts) \
            .withColumn("crnt_flag",   F.lit(True)) \
            .withColumn("inserted_at", F.current_timestamp()) \
            .withColumn("updated_at",  F.current_timestamp()) \
            .write.format("delta").mode("overwrite").saveAsTable(target_table)

    logger.info(f"SCD2 merge complete on {target_table}: {inserts_updates.count()} upserts, {deletes.count()} closures.")

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 4 — Mark records as processed in Multiplexer
# ---------------------------------------------------------------------------

def mark_processed(processed_ids: list) -> None:
    """
    Update processed_flag = True for all Multiplexer records that have been
    successfully routed. Uses a batch UPDATE via foreachBatch-safe pattern.
    """
    if not processed_ids:
        return
    id_list = ", ".join([f"'{i}'" for i in processed_ids])
    spark.sql(f"""
        UPDATE {MULTIPLEXER_TABLE}
        SET    processed_flag = true,
               processed_ts   = current_timestamp()
        WHERE  msg_id IN ({id_list})
    """)
    logger.info(f"Marked {len(processed_ids)} Multiplexer records as processed.")

# COMMAND ----------
# ---------------------------------------------------------------------------
# MAIN EXECUTION — Routing loop
# ---------------------------------------------------------------------------

def run_routing_job(spark: SparkSession) -> None:
    logger.info("Starting Multiplexer routing job …")

    pending_df  = load_unprocessed_records(spark)
    routing_cfg = load_routing_config(spark)
    total       = pending_df.count()

    if total == 0:
        logger.info("No unprocessed Multiplexer records. Exiting.")
        return

    logger.info(f"Found {total} unprocessed records across targets: {list(routing_cfg.keys())}")
    processed_ids = []

    for target_table, cfg in routing_cfg.items():
        table_records = pending_df.filter(F.col("target_table") == target_table)
        count = table_records.count()
        if count == 0:
            continue

        logger.info(f"Routing {count} records → {target_table} (scd_type={cfg['scd_type']})")
        business_key_cols = [k.strip() for k in cfg["business_key_cols"].split(",")]

        if cfg["scd_type"] == "SCD1":
            apply_scd1_merge(table_records, target_table, business_key_cols)
        elif cfg["scd_type"] == "SCD2":
            apply_scd2_merge(table_records, target_table, business_key_cols)
        else:
            logger.warning(f"Unknown scd_type '{cfg['scd_type']}' for {target_table} — skipping.")
            continue

        # Collect processed msg_ids to mark in Multiplexer
        ids = [r["msg_id"] for r in table_records.select("msg_id").collect()]
        processed_ids.extend(ids)

    mark_processed(processed_ids)
    logger.info(f"Routing job complete. Processed {len(processed_ids)}/{total} records.")


run_routing_job(spark)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone
import logging

logger = logging.getLogger("silver_to_gold")
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
SILVER_TABLE       = "catalog.silver.transactions"
GOLD_FACT_TABLE    = "catalog.gold.transactions_daily"
GOLD_DIM_TABLE     = "catalog.gold.dim_account_history"  # SCD2
CHECKPOINT_FACT    = "abfss://checkpoints@<storage>.dfs.core.windows.net/gold/transactions_daily/"
CHECKPOINT_DIM     = "abfss://checkpoints@<storage>.dfs.core.windows.net/gold/dim_account/"

HIGH_DATE   = "9999-12-31T23:59:59"
ACCOUNT_KEY = ["account_id"]
ACCOUNT_ATTRS = ["customer_id", "region", "channel"]  # attributes tracked in SCD2

# COMMAND ----------
# ---------------------------------------------------------------------------
# GOLD FACT — Daily transaction aggregates per account
# ---------------------------------------------------------------------------

def build_daily_fact(df: DataFrame) -> DataFrame:
    """
    Aggregate Silver transactions into daily fact records per account.
    Used for downstream BI reporting and reconciliation.
    """
    return (
        df
        .groupBy(
            F.col("account_id"),
            F.col("event_date"),
            F.col("transaction_type"),
            F.col("currency"),
            F.col("status")
        )
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.min("amount").alias("min_amount"),
            F.countDistinct("merchant_id").alias("unique_merchants"),
            F.max("event_ts").alias("latest_event_ts")
        )
        .withColumn("load_ts", F.current_timestamp())
    )


def upsert_daily_fact(batch_df: DataFrame, batch_id: int) -> None:
    """Upsert daily fact records (SCD1 — latest aggregation wins per day)."""
    fact_df = build_daily_fact(batch_df)

    if DeltaTable.isDeltaTable(spark, GOLD_FACT_TABLE):
        DeltaTable.forName(spark, GOLD_FACT_TABLE).alias("t") \
            .merge(
                fact_df.alias("s"),
                "t.account_id = s.account_id AND t.event_date = s.event_date "
                "AND t.transaction_type = s.transaction_type AND t.currency = s.currency "
                "AND t.status = s.status"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        fact_df.write.format("delta").mode("overwrite") \
               .partitionBy("event_date").saveAsTable(GOLD_FACT_TABLE)

    logger.info(f"Batch {batch_id}: Gold fact updated. Rows: {fact_df.count()}")

# COMMAND ----------
# ---------------------------------------------------------------------------
# GOLD DIMENSION — Account SCD Type 2 history
# ---------------------------------------------------------------------------

def add_dim_row_hash(df: DataFrame, attr_cols: list) -> DataFrame:
    """Compute MD5 hash over account attribute columns."""
    return df.withColumn(
        "row_hash",
        F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in attr_cols]))
    )


def apply_dim_scd2(batch_df: DataFrame, batch_id: int) -> None:
    """
    Maintain SCD Type 2 on the account dimension table.
    - Detects attribute changes via row_hash comparison
    - Expires the old record (sets eff_to = now, crnt_flag = false)
    - Inserts new version as the active record
    """
    # Get latest account attributes per account_id in this batch
    w = Window.partitionBy("account_id").orderBy(F.col("event_ts").desc())
    latest_accounts = (
        batch_df
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .select("account_id", "customer_id", "region", "channel")
        .drop("_rank")
    )
    latest_accounts = add_dim_row_hash(latest_accounts, ACCOUNT_ATTRS)

    if not DeltaTable.isDeltaTable(spark, GOLD_DIM_TABLE):
        # First load — insert all as current
        latest_accounts \
            .withColumn("eff_from",    F.current_timestamp()) \
            .withColumn("eff_to",      F.lit(HIGH_DATE).cast("timestamp")) \
            .withColumn("crnt_flag",   F.lit(True)) \
            .withColumn("inserted_at", F.current_timestamp()) \
            .withColumn("updated_at",  F.current_timestamp()) \
            .write.format("delta").mode("overwrite") \
            .saveAsTable(GOLD_DIM_TABLE)
        logger.info(f"Batch {batch_id}: Dimension table created with {latest_accounts.count()} records.")
        return

    delta_dim = DeltaTable.forName(spark, GOLD_DIM_TABLE)

    # Step 1: Expire changed records
    (
        delta_dim.alias("t")
        .merge(
            latest_accounts.alias("s"),
            "t.account_id = s.account_id AND t.crnt_flag = true AND t.row_hash != s.row_hash"
        )
        .whenMatchedUpdate(set={
            "crnt_flag":  F.lit(False),
            "eff_to":     F.current_timestamp(),
            "updated_at": F.current_timestamp()
        })
        .execute()
    )

    # Step 2: Insert new versions for changed + brand-new accounts
    existing_active = spark.table(GOLD_DIM_TABLE) \
                           .filter(F.col("crnt_flag") == True) \
                           .select("account_id", "row_hash") \
                           .withColumnRenamed("row_hash", "existing_hash")

    new_records = (
        latest_accounts
        .join(existing_active, on="account_id", how="left")
        .filter(
            F.col("existing_hash").isNull() |
            (F.col("row_hash") != F.col("existing_hash"))
        )
        .drop("existing_hash")
        .withColumn("eff_from",    F.current_timestamp())
        .withColumn("eff_to",      F.lit(HIGH_DATE).cast("timestamp"))
        .withColumn("crnt_flag",   F.lit(True))
        .withColumn("inserted_at", F.current_timestamp())
        .withColumn("updated_at",  F.current_timestamp())
    )

    if new_records.count() > 0:
        new_records.write.format("delta").mode("append") \
                   .saveAsTable(GOLD_DIM_TABLE)
        logger.info(f"Batch {batch_id}: {new_records.count()} new/updated dimension records inserted.")
    else:
        logger.info(f"Batch {batch_id}: No dimension changes detected.")

# COMMAND ----------
# ---------------------------------------------------------------------------
# RECONCILIATION CHECK
# ---------------------------------------------------------------------------

def run_reconciliation() -> None:
    """
    Validate Gold totals vs Silver source to ensure no data loss.
    Writes reconciliation results to monitoring table.
    """
    silver_total = spark.table(SILVER_TABLE).agg(F.sum("amount").alias("s")).collect()[0]["s"]
    gold_total   = spark.table(GOLD_FACT_TABLE).agg(F.sum("total_amount").alias("g")).collect()[0]["g"]
    diff         = abs((silver_total or 0) - (gold_total or 0))
    status       = "PASS" if diff < 0.01 else "FAIL"

    logger.info(f"Reconciliation: Silver={silver_total:.2f}, Gold={gold_total:.2f}, Diff={diff:.4f}, Status={status}")

    recon_data = [{
        "run_ts":        datetime.now(timezone.utc).isoformat(),
        "silver_total":  float(silver_total or 0),
        "gold_total":    float(gold_total or 0),
        "difference":    float(diff),
        "status":        status
    }]
    spark.createDataFrame(recon_data) \
         .write.format("delta").mode("append") \
         .saveAsTable("catalog.monitoring.reconciliation_results")

# COMMAND ----------
# ---------------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting Silver → Gold streaming job …")

    silver_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .table(SILVER_TABLE)
    )

    def process_gold_batch(batch_df: DataFrame, batch_id: int) -> None:
        upsert_daily_fact(batch_df, batch_id)
        apply_dim_scd2(batch_df, batch_id)

    query = (
        silver_stream.writeStream
        .foreachBatch(process_gold_batch)
        .option("checkpointLocation", CHECKPOINT_FACT)
        .trigger(processingTime="5 minutes")
        .start()
    )

    logger.info(f"Gold stream query ID: {query.id}")
    # query.awaitTermination()


main()
