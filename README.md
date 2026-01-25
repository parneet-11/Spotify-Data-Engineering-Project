# Spotify End-to-End Data Engineering Project

This project demonstrates a production-grade, metadata-driven data pipeline built on the Azure ecosystem and Databricks. It follows the Medallion Architecture to process Spotify-themed data from raw ingestion to a business-ready Gold layer.

## 📌 Architecture
The pipeline implements a modern data stack focusing on scalability, data governance, and CI/CD.

- **Ingestion:** Azure SQL Database ➡️ Azure Data Factory (ADF)
- **Storage:** Azure Data Lake Storage Gen2 (ADLS)
- **Processing:** Azure Databricks (Spark & Delta Live Tables)
- **Governance:** Unity Catalog
- **DevOps:** Databricks Asset Bundles (DABs) & GitHub CI/CD


## 🛠️ Technical Deep Dive
- **1. Ingestion Layer (Bronze)**
  - We utilize Azure Data Factory to perform metadata-driven ingestion from an Azure SQL Database.
  - **Incremental Loading:** Implemented a high-watermark logic using a CDC (Change Data Capture) JSON file to track the last processed timestamp.
  - **Backfilling:** The pipeline is designed to retroactively process historical data by passing dynamic parameters.
  - **Format:** Data is landing in the Bronze container in Delta Lake format to ensure ACID transactions.
- **2. Transformation Layer (Silver)**
  - Data is refined using Databricks Autoloader (cloudFiles) for efficient ingestion into the Silver layer.
  - **Schema Evolution:** Leveraged Autoloader to handle source schema changes and capture "rescued data."
  - **Jinja2 Templates:** Developed a "SQL-writing machine" using Python's Jinja2 library. This allows for dynamic SQL generation for joins and transformations, adhering to the DRY (Don't Repeat Yourself) principle.
  - **Idempotency:** Pipelines are engineered to be re-runnable without creating duplicate records or data corruption.
- **3. Business Layer (Gold)**
  - The Gold layer utilizes Delta Live Tables (DLT), a declarative framework for building reliable data pipelines.
  - **Data Quality (Expectations):** Integrated data quality gates to warn, drop, or fail the pipeline based on record validity.
  - **SCD Type 2:** Implemented Slowly Changing Dimensions (SCD Type 2) using create_auto_cdc_flow to track the history of user subscriptions and artist genres over time.

  
## 🔐 Governance & Security
  - **Unity Catalog:** Managed security via a three-level namespace (catalog.schema.table). This provides centralized access control and data lineage.
  - **Access Connector:** Used as an "identity bridge" between Databricks and ADLS Gen2, ensuring secure, credential-less data access.
## 🚀 DevOps & CI/CD
- **Databricks Asset Bundles (DABs):** The entire Databricks environment (clusters, jobs, DLT pipelines) is managed as Infrastructure as Code (IaC).
- **ADF CI/CD:** Connected ADF to GitHub using a feature-branch strategy. Merges trigger the generation of ARM templates in the adf_publish branch for automated deployment.
- **Alerting:** Integrated Azure Logic Apps to send automated email alerts via Outlook on pipeline success or failure.
## 📊 Data Model
The dataset consists of a Star Schema:

- **FactStream:** Streaming events (1k records).
- **DimUser, DimArtist, DimTrack:** 500 records each.
- **DimDate:** Time dimension for temporal analysis.

## 🛠️ Key Features
- **Metadata-Driven:** The ETL process is controlled via a centralized metadata table in Azure SQL DB, allowing for dynamic pipeline execution without hardcoding table names or paths.
- **Automated Ingestion:** Automated triggers to fetch daily trending tracks and artist data from the Spotify Web API.
- **Layered Data Processing:** Implements a Medallion Architecture (Bronze, Silver, Gold layers) for data quality.

## 📂 Project Structure
```text
├── factory/                # ADF JSON definitions (Datasets & Linked Services)
├── pipeline/               # ADF Pipeline definitions (Incremental Ingestion logic)
├── dataset/                # ADF Dataset JSON definitions (AzureSqlTable, Dynamic JSON/Parquet)
├── Jinja/                  # Jinja2 SQL templates for dynamic query generation
├── src/                    # Main source code for Databricks/Spark jobs
├── utils/                  # Shared helper classes and reusable Python functions
├── resources/              # Configuration files for DLT pipelines and jobs
├── linkedService/          # ADF Linked Service configurations (Data Lake, SQL, etc.)
├── images/                 # Architecture diagrams and documentation assets
├── databricks.yml          # Root configuration for Databricks Asset Bundles (DABs)
├── pyproject.toml          # Python project dependencies and build settings
├── publish_config.json     # Configuration for ADF deployment/publishing
└── .gitignore              # Files and patterns to be excluded from version control
└── .gitignore              # Files and patterns to be excluded from version control


