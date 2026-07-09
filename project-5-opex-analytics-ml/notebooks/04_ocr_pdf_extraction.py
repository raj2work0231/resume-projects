# =============================================================================
# Project 5 – OPEX Analytics
# Notebook: 04_pdf_regex_extraction.py
#
# Purpose : Extract structured data from supplier PDF invoices using pdfplumber
#           for text extraction and regex patterns for field parsing.
#           Output written to a Bronze Delta table for downstream processing.
#
# Stack   : Azure Databricks | Python | pdfplumber | PySpark
# =============================================================================

# %pip install pdfplumber azure-storage-blob

import pdfplumber
import re, os, logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType
)

logger = logging.getLogger("pdf_regex_extraction")
spark  = SparkSession.builder.getOrCreate()

PDF_LANDING_PATH    = "abfss://raw@<storage>.dfs.core.windows.net/invoices/"
EXTRACTED_TABLE     = "catalog.bronze.invoice_extracted"
FAILED_TABLE        = "catalog.bronze.invoice_extraction_failed"
LOCAL_PDF_DIR       = "/tmp/pdf_staging/"

# ---------------------------------------------------------------------------
# PDF TEXT EXTRACTION — digital PDFs
# ---------------------------------------------------------------------------

def extract_text_pdfplumber(pdf_path: str) -> list[dict]:
    """
    Extract text from all pages of a digital (text-selectable) PDF.
    Returns list of {page_num, text} dicts.
    """
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            # Also extract tables if present
            tables = page.extract_tables()
            pages.append({
                "page_num": i + 1,
                "text":     text,
                "tables":   tables
            })
    return pages

# ---------------------------------------------------------------------------
# OCR EXTRACTION — scanned / image-based PDFs
# ---------------------------------------------------------------------------

def extract_text_ocr(pdf_path: str, dpi: int = 300) -> list[dict]:
    """
    Convert PDF pages to images and run Tesseract OCR.
    Used for scanned invoices that are not text-selectable.
    """
    pages = []
    doc   = fitz.open(pdf_path)
    for i, page in enumerate(doc):
        mat    = fitz.Matrix(dpi / 72, dpi / 72)   # scale factor for DPI
        pix    = page.get_pixmap(matrix=mat)
        img    = Image.open(io.BytesIO(pix.tobytes("png")))
        text   = pytesseract.image_to_string(img, config="--oem 3 --psm 6")
        pages.append({"page_num": i + 1, "text": text, "tables": []})
    doc.close()
    return pages

# ---------------------------------------------------------------------------
# FIELD EXTRACTION — regex-based structured field parsing
# ---------------------------------------------------------------------------

# Regex patterns for common invoice fields
INVOICE_PATTERNS = {
    "invoice_number": r"(?:Invoice\s*(?:No\.?|Number|#)\s*[:\-]?\s*)([A-Z0-9\-]+)",
    "invoice_date":   r"(?:Invoice\s*Date\s*[:\-]?\s*)(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})",
    "total_amount":   r"(?:Total\s*(?:Amount|Due|Payable)\s*[:\-]?\s*\$?)([\d,]+\.?\d*)",
    "supplier_name":  r"(?:From\s*[:\-]?\s*|Vendor\s*[:\-]?\s*)([A-Za-z\s&,\.]+)(?:\n|\r)",
    "account_number": r"(?:Account\s*(?:No\.?|Number|#)\s*[:\-]?\s*)([A-Z0-9\-]+)",
    "due_date":       r"(?:(?:Payment\s*)?Due\s*Date\s*[:\-]?\s*)(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})",
    "gst_amount":     r"(?:GST\s*[:\-]?\s*\$?)([\d,]+\.?\d*)",
    "subtotal":       r"(?:Sub\s*Total\s*[:\-]?\s*\$?)([\d,]+\.?\d*)",
}


def extract_fields(text: str) -> dict:
    """
    Apply regex patterns to extract structured fields from raw invoice text.
    Returns a dict of field_name → extracted_value.
    """
    fields = {}
    for field, pattern in INVOICE_PATTERNS.items():
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        fields[field] = match.group(1).strip() if match else None
    return fields


def clean_amount(raw: str) -> float:
    """Parse a currency string like '1,234.56' to float."""
    if raw is None:
        return None
    try:
        return float(raw.replace(",", "").replace("$", "").strip())
    except ValueError:
        return None

# ---------------------------------------------------------------------------
# DETECT IF PDF IS SCANNED OR DIGITAL
# ---------------------------------------------------------------------------

def is_scanned_pdf(pdf_path: str) -> bool:
    """
    Heuristic: if total extracted text < 100 chars across all pages,
    assume the PDF is scanned and needs OCR.
    """
    with pdfplumber.open(pdf_path) as pdf:
        total_text = "".join(p.extract_text() or "" for p in pdf.pages)
    return len(total_text.strip()) < 100

# ---------------------------------------------------------------------------
# PROCESS A SINGLE PDF
# ---------------------------------------------------------------------------

def process_pdf(pdf_path: str, filename: str) -> dict:
    """
    End-to-end processing of a single PDF:
    1. Detect digital vs scanned
    2. Extract text
    3. Parse structured fields
    4. Return structured record
    """
    try:
        if is_scanned_pdf(pdf_path):
            logger.info(f"  {filename}: scanned PDF — running OCR")
            pages = extract_text_ocr(pdf_path)
        else:
            logger.info(f"  {filename}: digital PDF — extracting text")
            pages = extract_text_pdfplumber(pdf_path)

        full_text = "\n".join(p["text"] for p in pages)
        fields    = extract_fields(full_text)

        return {
            "filename":       filename,
            "invoice_number": fields.get("invoice_number"),
            "invoice_date":   fields.get("invoice_date"),
            "due_date":       fields.get("due_date"),
            "supplier_name":  fields.get("supplier_name"),
            "account_number": fields.get("account_number"),
            "subtotal":       clean_amount(fields.get("subtotal")),
            "gst_amount":     clean_amount(fields.get("gst_amount")),
            "total_amount":   clean_amount(fields.get("total_amount")),
            "page_count":     len(pages),
            "extraction_method": "OCR" if is_scanned_pdf(pdf_path) else "TEXT",
            "extracted_at":   datetime.utcnow().isoformat(),
            "status":         "SUCCESS",
            "error":          None
        }
    except Exception as e:
        logger.error(f"  {filename}: extraction failed — {e}")
        return {
            "filename": filename, "status": "FAILED", "error": str(e),
            **{k: None for k in ["invoice_number","invoice_date","due_date",
                                   "supplier_name","account_number","subtotal",
                                   "gst_amount","total_amount","page_count","extraction_method","extracted_at"]}
        }

# ---------------------------------------------------------------------------
# BATCH PROCESS ALL PDFs
# ---------------------------------------------------------------------------

def process_pdf_batch(spark: SparkSession, pdf_dir: str) -> None:
    """
    Process all PDF files in the staging directory and write results to Delta.
    """
    pdf_files = [f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")]
    logger.info(f"Processing {len(pdf_files)} PDFs …")

    records = [process_pdf(os.path.join(pdf_dir, f), f) for f in pdf_files]

    success = [r for r in records if r["status"] == "SUCCESS"]
    failed  = [r for r in records if r["status"] == "FAILED"]

    if success:
        spark.createDataFrame(success) \
             .withColumn("ingest_date", F.to_date(F.current_timestamp())) \
             .write.format("delta").mode("append") \
             .saveAsTable(EXTRACTED_TABLE)
        logger.info(f"Extracted {len(success)} invoices successfully.")

    if failed:
        spark.createDataFrame(failed) \
             .write.format("delta").mode("append") \
             .saveAsTable(FAILED_TABLE)
        logger.warning(f"{len(failed)} PDFs failed extraction — see {FAILED_TABLE}")


if __name__ == "__main__":
    os.makedirs(LOCAL_PDF_DIR, exist_ok=True)
    # In production: download PDFs from ADLS using dbutils.fs or azure SDK
    # dbutils.fs.cp(PDF_LANDING_PATH, f"file://{LOCAL_PDF_DIR}", recurse=True)
    process_pdf_batch(spark, LOCAL_PDF_DIR)
