# Data Engineering Portfolio — Azure Databricks & PySpark
<img width="502" height="397" alt="image" src="https://github.com/user-attachments/assets/39d6691d-7614-4851-81f5-0c658a2b9bd8" />
<img width="474" height="242" alt="image" src="https://github.com/user-attachments/assets/28f5d507-52d4-4cc1-b954-49e575cbcefb" />
<img width="480" height="260" alt="image" src="https://github.com/user-attachments/assets/83d8b37c-ec9a-460a-b811-2fd7fb32b910" />
<img width="463" height="234" alt="image" src="https://github.com/user-attachments/assets/49d50d8d-5c1d-4fcf-9956-11c6d6555b2b" />



> **Senior Data Engineer** | 7+ years building production-grade pipelines on Azure Databricks, Delta Lake, Apache Kafka, and PySpark for large-scale enterprise workloads.

---

## Projects

| # | Project | Period | Key Technologies |
|---|---------|--------|-----------------|
| 1 | [Geo-Location Intelligence & Audience Segmentation Platform](project-1-geo-intelligence/) | 2025 – Present | AutoLoader, Uber H3, Voronoi, SCD2, Photon, AQE, Liquid Clustering |
| 2 | [Real-Time Kafka Streaming Pipeline (Medallion Architecture)](project-2-kafka-streaming/) | 2024 – 2025 | Kafka, Spark Structured Streaming, SCD1/2, Delta Lake, Data Quality |
| 3 | [Metadata-Driven AutoLoader Ingestion Framework](project-3-autoloader-framework/) | 2022 – 2023 | AutoLoader File-Notification, Schema Evolution, 70%+ Cost Reduction |
| 4 | [Enterprise Multi-Source ETL Pipeline Platform](project-4-enterprise-etl/) | 2021 – 2022 | ADF, SFTP, PGP Encryption, Databricks Workflows, Delta Tables |
| 5 | [OPEX Analytics, Geospatial Dashboards & ML Enrichment](project-5-opex-analytics-ml/) | 2019 – 2021 | Uber H3, OCR/PDF, Random Forest, MLflow, Power BI |

---

## Repository Structure

```
github-portfolio/
├── index.html                              # Portfolio landing page
├── project-1-geo-intelligence/
│   ├── architecture.html                   # Architecture diagram & documentation
│   ├── notebooks/
│   │   ├── 01_autoloader_mediation_ingestion.py
│   │   ├── 02_h3_geospatial_processing.py
│   │   ├── 03_scd2_delta_management.py
│   │   ├── 04_parent_child_orchestration.py
│   │   └── 05_spark_performance_tuning.py
│   └── sql/
│       ├── 01_create_delta_tables.sql
│       └── 02_scd2_merge_query.sql
├── project-2-kafka-streaming/
│   ├── architecture.html
│   ├── notebooks/
│   │   ├── 01_kafka_stream_consumer.py
│   │   ├── 02_bronze_to_silver.py
│   │   └── 03_silver_to_gold_scd2.py
│   └── sql/
│       ├── 01_create_streaming_tables.sql
│       └── 02_silver_gold_transforms.sql
├── project-3-autoloader-framework/
│   ├── architecture.html
│   ├── notebooks/
│   │   ├── 01_autoloader_file_notification.py
│   │   ├── 02_schema_evolution_handler.py
│   │   └── 03_scd_orchestration.py
│   └── sql/
│       └── 01_metadata_tables.sql
├── project-4-enterprise-etl/
│   ├── architecture.html
│   ├── notebooks/
│   │   ├── 01_sftp_pgp_ingestion.py
│   │   └── 02_raw_to_curated.py
│   └── sql/
│       └── 01_tables_and_views.sql
└── project-5-opex-analytics-ml/
    ├── architecture.html
    ├── notebooks/
    │   ├── 01_opex_data_ingestion.py
    │   ├── 02_business_rules_cost_attribution.py
    │   ├── 03_h3_geospatial_analytics.py
    │   ├── 04_ocr_pdf_extraction.py
    │   └── 05_random_forest_ml_enrichment.py
    └── sql/
        └── 01_analytics_queries.sql
```

---

## Core Skills Demonstrated

**Ingestion**
- Databricks AutoLoader (file-notification & directory-listing modes)
- Apache Kafka + Spark Structured Streaming (foreachBatch, trigger modes)
- SFTP + PGP decryption pipelines
- Azure Data Factory pipeline orchestration

**Transformation & Modeling**
- Medallion Architecture (Bronze → Silver → Gold)
- SCD Type 1 & Type 2 with Delta MERGE INTO
- Schema evolution & rescue data patterns
- Metadata-driven, configuration-driven frameworks

**Geospatial**
- Uber H3 hexagonal indexing (resolutions 7–11)
- Voronoi cell-site polygon mapping
- POI enrichment & OpenStreetMap integration
- Geospatial cost density analytics

**Performance & Optimization**
- Adaptive Query Execution (AQE)
- Photon vectorized engine
- Liquid Clustering & Z-ORDER
- Optimise-write & auto-compaction

**Machine Learning**
- Scikit-learn Random Forest (classification & regression)
- MLflow experiment tracking
- Feature engineering & imputation

**Document AI**
- OCR with Tesseract
- PDF text extraction (pdfplumber, PyMuPDF)
- Regex-based field parsing for invoices

**Governance & Security**
- Unity Catalog fine-grained permissions & lineage
- Azure RBAC + ADLS Gen2 encryption
- PGP encryption/decryption via Azure Key Vault

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Cloud | Azure (Databricks, ADLS Gen2, ADF, Key Vault, Event Grid) |
| Compute | Azure Databricks (Photon runtime) |
| Storage | Delta Lake on ADLS Gen2 |
| Streaming | Apache Kafka, Spark Structured Streaming |
| Languages | Python, PySpark, SparkSQL |
| Geospatial | Uber H3, GeoPandas, Shapely, OSMnx |
| ML | Scikit-learn, MLflow, Pandas, NumPy |
| Doc AI | pdfplumber, PyMuPDF, Tesseract OCR |
| Governance | Unity Catalog, Azure RBAC |
| Orchestration | Databricks Workflows, Azure Data Factory, Control-M |
| Visualization | Power BI |

---

*All code is a portfolio POC — replace placeholder values (storage account names, table names, secret scope names) with your actual environment configuration before running.*
