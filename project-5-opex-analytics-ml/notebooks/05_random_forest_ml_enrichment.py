# =============================================================================
# Project 5 – OPEX Analytics
# Notebook: 05_metadata_driven_pdf_framework.py
#
# Purpose : Metadata-driven PDF extraction framework.
#           Each supplier / invoice type is registered as a config row in the
#           pdf_extraction_config Delta table (extraction mode, regex field
#           patterns, filename routing pattern). The framework reads active
#           configs, uses pdfplumber to extract raw text from each PDF, applies
#           supplier-specific regex patterns to parse structured fields,
#           writes a unified Silver invoicing_dataset Delta table.
#           The invoicing dataset is then joined with the infrastructure power
#           supply asset register (theoretical contract cost per asset /
#           billing period) to compute variance and flag contracts as
#           RECALCULATE / WITHIN_TOLERANCE / UNDERBILLED — providing evidence
#           for supplier contract recalculation.
#
# Stack   : Azure Databricks | PySpark | pdfplumber | Delta Lake
# =============================================================================

# %pip install pdfplumber

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import pdfplumber
import os, re, json, logging
from datetime import datetime

logger = logging.getLogger("pdf_extraction_framework")
spark  = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
PDF_CONFIG_TABLE      = "catalog.control.pdf_extraction_config"
PDF_LANDING_PATH      = "abfss://raw@<storage>.dfs.core.windows.net/invoices/"
INVOICE_TABLE         = "catalog.silver.invoicing_dataset"        # normalised extracted invoices
INFRA_ASSET_TABLE     = "catalog.gold.infra_power_supply_assets"  # theoretical contract costs
CONTRACT_TABLE        = "catalog.gold.contract_cost_analysis"     # variance + contract action flags

COST_VARIANCE_THRESHOLD = 0.05   # flag RECALCULATE if actual > theoretical by > 5%

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 1 — Load supplier extraction configs from metadata table
# ---------------------------------------------------------------------------

def load_extraction_configs(spark: SparkSession) -> list:
    """
    Load active rows from pdf_extraction_config.
    Each row defines how to extract invoice fields for one supplier / invoice type:
      supplier_id       : unique supplier code
      invoice_type      : e.g. POWER_SUPPLY, POLE_RENTAL
      field_patterns_json: JSON map of { field_name: regex_pattern }
      filename_pattern  : regex to match PDF filenames to this config
    """
    return [
        row.asDict()
        for row in spark.table(PDF_CONFIG_TABLE).filter(F.col("is_active") == True).collect()
    ]

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 2 — Extract invoice fields using supplier-specific config
# ---------------------------------------------------------------------------

def extract_invoice_fields(pdf_path: str, config: dict) -> dict:
    """
    Extract structured invoice fields from a PDF using the matched supplier config.
    Uses pdfplumber to extract raw text from all pages, then applies supplier-specific
    regex patterns to parse field values from the extracted text.
    Returns a dict matching the invoicing_dataset schema.
    """
    supplier_id    = config["supplier_id"]
    field_patterns = json.loads(config.get("field_patterns_json", "{}"))

    raw_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            raw_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as exc:
        logger.error(f"PDF text extraction failed for {os.path.basename(pdf_path)}: {exc}")
        return {}

    # Apply regex patterns to extract field values from raw text
    extracted = {
        "supplier_id":  supplier_id,
        "invoice_type": config.get("invoice_type", "UNKNOWN"),
        "source_file":  os.path.basename(pdf_path),
        "extracted_at": datetime.utcnow().isoformat(),
    }
    for field, pattern in field_patterns.items():
        match = re.search(pattern, raw_text, re.IGNORECASE)
        extracted[field] = match.group(1).strip() if match else None

    return extracted

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 3 — Normalise and write to invoicing_dataset Delta table
# ---------------------------------------------------------------------------

def normalise_and_write(spark: SparkSession, records: list, config: dict) -> None:
    """
    Cast extracted string fields to typed columns, apply supplier normalisation
    rules, and append to the shared invoicing_dataset Silver table.
    """
    if not records:
        logger.info(f"[{config['supplier_id']}] No records to write.")
        return

    df = spark.createDataFrame(records)

    # Normalise monetary amounts (strip commas, cast to double)
    if "total_amount" in df.columns:
        df = df.withColumn("total_amount",
            F.regexp_replace(F.col("total_amount"), "[,$]", "").cast("double"))

    # Parse date fields from supplier-specific formats
    for date_col, fmt in [("invoice_date", "MM/dd/yyyy"), ("billing_period", "MM/yyyy")]:
        if date_col in df.columns:
            df = df.withColumn(date_col, F.to_date(F.col(date_col), fmt))

    df = df.withColumn("load_ts", F.current_timestamp())

    df.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(INVOICE_TABLE)

    logger.info(f"[{config['supplier_id']}] {df.count()} invoice records written to {INVOICE_TABLE}")

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 4 — Orchestrate metadata-driven extraction for all supplier PDFs
# ---------------------------------------------------------------------------

def run_extraction_framework(spark: SparkSession, pdf_paths: list) -> None:
    """
    For each PDF file:
      1. Match to a supplier config by filename_pattern regex
      2. Extract raw text via pdfplumber and parse fields using supplier regex patterns
      3. Normalise and append to invoicing_dataset
    Unmatched files are logged as warnings.
    """
    configs = load_extraction_configs(spark)

    for pdf_path in pdf_paths:
        matched_config = next(
            (cfg for cfg in configs
             if cfg.get("filename_pattern") and
                re.search(cfg["filename_pattern"], os.path.basename(pdf_path), re.IGNORECASE)),
            None
        )
        if not matched_config:
            logger.warning(f"No supplier config matched: {os.path.basename(pdf_path)}")
            continue

        record = extract_invoice_fields(pdf_path, matched_config)
        if record:
            normalise_and_write(spark, [record], matched_config)

# COMMAND ----------
# ---------------------------------------------------------------------------
# STEP 5 — Contract cost analysis: invoicing dataset × power supply asset register
# ---------------------------------------------------------------------------

def run_contract_analysis(spark: SparkSession) -> None:
    """
    Join the invoicing dataset (actual billed amounts) against the infrastructure
    power supply asset register (theoretical contract cost per asset / billing period).
    Compute cost variance and flag each contract line as:
      RECALCULATE    — actual > theoretical by more than COST_VARIANCE_THRESHOLD
      UNDERBILLED    — actual < theoretical cost (supplier undercharged)
      WITHIN_TOLERANCE — within acceptable variance band
    Output written to contract_cost_analysis Delta table for Power BI reporting
    and supplier renegotiation evidence.
    """
    invoices_df = spark.table(INVOICE_TABLE)
    infra_df    = spark.table(INFRA_ASSET_TABLE)

    analysis = (
        invoices_df.alias("i")
        .join(infra_df.alias("a"), on=["supplier_id", "billing_period"], how="inner")
        .select(
            F.col("i.supplier_id"),
            F.col("i.invoice_number"),
            F.col("i.invoice_type"),
            F.col("i.billing_period"),
            F.col("a.asset_id"),
            F.col("a.asset_type"),
            F.col("a.region"),
            F.col("a.theoretical_monthly_cost").alias("theoretical_cost"),
            F.col("i.total_amount").alias("actual_invoiced_cost"),
            (F.col("i.total_amount") - F.col("a.theoretical_monthly_cost"))
              .alias("cost_variance"),
            (
                (F.col("i.total_amount") - F.col("a.theoretical_monthly_cost"))
                / F.col("a.theoretical_monthly_cost")
            ).alias("variance_pct"),
        )
        .withColumn(
            "contract_action",
            F.when(F.col("variance_pct") >  F.lit(COST_VARIANCE_THRESHOLD),  F.lit("RECALCULATE"))
             .when(F.col("variance_pct") < -F.lit(COST_VARIANCE_THRESHOLD),  F.lit("UNDERBILLED"))
             .otherwise(F.lit("WITHIN_TOLERANCE"))
        )
        .withColumn("analysis_ts", F.current_timestamp())
    )

    recalc_count = analysis.filter(F.col("contract_action") == "RECALCULATE").count()
    total        = analysis.count()
    logger.info(
        f"Contract analysis: {recalc_count:,} / {total:,} contracts flagged RECALCULATE "
        f"(threshold >{COST_VARIANCE_THRESHOLD:.0%})"
    )

    analysis.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("contract_action", "billing_period") \
            .saveAsTable(CONTRACT_TABLE)

    logger.info(f"Contract analysis written to {CONTRACT_TABLE}")

# COMMAND ----------
# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main(spark: SparkSession) -> None:
    logger.info("Starting metadata-driven PDF extraction + contract cost analysis …")

    # Discover all PDF files in the ADLS Gen2 landing path
    pdf_paths = [
        row["path"]
        for row in spark.read.format("binaryFile")
                        .load(PDF_LANDING_PATH)
                        .select("path").collect()
    ]
    logger.info(f"Found {len(pdf_paths)} PDF files in landing zone.")

    run_extraction_framework(spark, pdf_paths)
    run_contract_analysis(spark)

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main(spark)
    """
    Split infrastructure asset records into:
