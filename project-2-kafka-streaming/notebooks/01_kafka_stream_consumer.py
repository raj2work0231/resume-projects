# =============================================================================
# Project 2 – Real-Time Kafka Streaming Pipeline
# Notebook: 01_kafka_stream_consumer.py
#
# Purpose : Consume real-time events from Apache Kafka using Spark Structured
#           Streaming. Parses JSON payloads, enforces schema, and writes raw
#           events as Parquet files to ADLS Gen2 (raw landing zone), partitioned
#           by Kafka topic and ingest date. No Delta transformation at this stage
#           — optimised for maximum ingestion throughput.
#
# Stack   : Azure Databricks | Apache Kafka | Spark Structured Streaming | Delta Lake
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    TimestampType, DoubleType, IntegerType
)
import logging

logger = logging.getLogger("kafka_consumer")
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION  (parameterise via Databricks widgets or job parameters)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "<your-kafka-broker>:9092"  # or Azure Event Hubs FQDN
KAFKA_TOPIC             = "events.transactions"
KAFKA_GROUP_ID          = "databricks-streaming-consumer"
KAFKA_STARTING_OFFSET   = "latest"   # "earliest" for backfill
KAFKA_MAX_OFFSETS_PER_TRIGGER = 50_000

# Raw Parquet landing zone on ADLS Gen2 (partitioned by topic and ingest_date)
PARQUET_LANDING_BASE    = "abfss://raw@<storage>.dfs.core.windows.net/kafka-landing/"
PARQUET_LANDING_PATH    = f"{PARQUET_LANDING_BASE}{KAFKA_TOPIC}/"
CHECKPOINT_PATH         = "abfss://checkpoints@<storage>.dfs.core.windows.net/kafka-landing/transactions/"
DEAD_LETTER_PATH        = "abfss://raw@<storage>.dfs.core.windows.net/dead-letter/transactions/"
TRIGGER_INTERVAL        = "30 seconds"

# For Azure Event Hubs Kafka endpoint — add SASL/SSL config:
KAFKA_SECURITY_OPTS = {
    "kafka.security.protocol":               "SASL_SSL",
    "kafka.sasl.mechanism":                  "PLAIN",
    "kafka.sasl.jaas.config":
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="$ConnectionString" '
        'password="<EVENT_HUB_CONNECTION_STRING>";',
}

# ---------------------------------------------------------------------------
# PAYLOAD SCHEMA — expected JSON structure in Kafka message value
# ---------------------------------------------------------------------------
transaction_schema = StructType([
    StructField("transaction_id",   StringType(),    False),
    StructField("account_id",       StringType(),    False),
    StructField("customer_id",      StringType(),    True),
    StructField("amount",           DoubleType(),    True),
    StructField("currency",         StringType(),    True),
    StructField("transaction_type", StringType(),    True),   # DEBIT / CREDIT
    StructField("merchant_id",      StringType(),    True),
    StructField("status",           StringType(),    True),   # PENDING / COMPLETED / FAILED
    StructField("channel",          StringType(),    True),   # ONLINE / MOBILE / BRANCH
    StructField("event_ts",         TimestampType(), True),
    StructField("region",           StringType(),    True),
])

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 1: Read from Kafka
# ---------------------------------------------------------------------------

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Create a Structured Streaming source from a Kafka topic.
    Returns a DataFrame with Kafka envelope columns (key, value, metadata).
    """
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         KAFKA_STARTING_OFFSET)
        .option("maxOffsetsPerTrigger",    KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss",          "false")   # tolerate topic retention gaps
    )
    # Apply security options for Azure Event Hubs / secured Kafka
    for k, v in KAFKA_SECURITY_OPTS.items():
        reader = reader.option(k, v)

    return reader.load()

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 2: Parse Kafka envelope → typed payload DataFrame
# ---------------------------------------------------------------------------

def parse_kafka_message(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Extract and parse the Kafka value (binary → JSON → typed struct).
    Also preserve Kafka metadata: topic, partition, offset, timestamp.
    """
    return (
        raw_df
        # Decode binary value to string
        .withColumn("value_str", F.col("value").cast(StringType()))
        # Parse JSON against schema
        .withColumn("payload", F.from_json(F.col("value_str"), schema))
        # Flatten payload struct into columns
        .select(
            "payload.*",
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_event_ts"),
            F.col("key").cast(StringType()).alias("kafka_message_key"),
        )
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 3: Add pipeline metadata columns
# ---------------------------------------------------------------------------

def add_pipeline_metadata(df: DataFrame) -> DataFrame:
    """Attach pipeline audit columns for traceability."""
    return (
        df
        .withColumn("row_hash",
            F.md5(F.concat_ws("||",
                F.col("transaction_id"), F.col("account_id"),
                F.col("amount").cast("string"), F.col("event_ts").cast("string")
            ))
        )
        .withColumn("ingest_ts",   F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
        .withColumn("pipeline_version", F.lit("1.0.0"))
    )

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 4: Dead-letter handling — quarantine unparseable records
# ---------------------------------------------------------------------------
DEAD_LETTER_TABLE = "catalog.bronze.transactions_dead_letter"

def split_valid_invalid(df: DataFrame) -> tuple:
    """
    Separate records where JSON parsing failed (payload is null)
    from valid parsed records.
    """
    valid   = df.filter(F.col("transaction_id").isNotNull())
    invalid = df.filter(F.col("transaction_id").isNull()) \
                .withColumn("quarantine_reason", F.lit("json_parse_failed")) \
                .withColumn("quarantine_ts",     F.current_timestamp())
    return valid, invalid

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 5: Write to ADLS Gen2 as Parquet (raw landing zone)
# ---------------------------------------------------------------------------

def write_parquet_stream(df: DataFrame, checkpoint: str, landing_path: str):
    """Write valid records to ADLS Gen2 as Parquet files via streaming append."""
    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .partitionBy("kafka_topic", "ingest_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(landing_path)
    )


def write_dead_letter(df: DataFrame, path: str):
    """Write invalid/quarantined records to dead-letter Parquet path."""
    def batch_writer(batch_df, batch_id):
        if batch_df.count() > 0:
            batch_df.write.format("parquet").mode("append").save(path)
            logger.warning(f"Dead-letter: {batch_df.count()} records quarantined in batch {batch_id}")
    return batch_writer

# COMMAND ----------
# ---------------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------------

def main():
    logger.info(f"Starting Kafka consumer for topic: {KAFKA_TOPIC}")

    raw_stream    = read_kafka_stream(spark)
    parsed_stream = parse_kafka_message(raw_stream, transaction_schema)
    enriched      = add_pipeline_metadata(parsed_stream)

    # Split valid vs dead-letter using foreachBatch
    def process_batch(batch_df: DataFrame, batch_id: int):
        valid_df, invalid_df = split_valid_invalid(batch_df)
        logger.info(f"Batch {batch_id}: valid={valid_df.count()}, invalid={invalid_df.count()}")

        # Write valid records to ADLS Gen2 Parquet landing zone
        valid_df.write \
                .format("parquet") \
                .mode("append") \
                .partitionBy("kafka_topic", "ingest_date") \
                .save(PARQUET_LANDING_PATH)

        # Write invalid records to dead-letter Parquet path
        if invalid_df.count() > 0:
            invalid_df.write.format("parquet").mode("append").save(DEAD_LETTER_PATH)

    query = (
        enriched.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(f"Parquet landing stream running. Query ID: {query.id}")
    # query.awaitTermination()


main()
