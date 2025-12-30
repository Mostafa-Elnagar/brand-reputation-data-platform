# Market Opinion Data Platform

**A Generic, Enterprise-Grade Batch Data Pipeline for Market Intelligence.**

This platform is a scalable, serverless data lakehouse framework designed to ingest, transform, and analyze unstructured social media data at scale. It demonstrates a production-ready **Batch Processing** architecture using AWS and Snowflake.

> **Showcase Implementation**: This repository contains a reference implementation for **Smartphone Market Intelligence**, allowing brands to track sentiment across features like *Camera*, *Battery*, and *Price*. However, the core engine is domain-agnostic and can be adapted for Automotive, Gaming, Finance, or any other vertical.

![Architecture](assets/architecture.jpg)

---

## 🏗️ Core Architecture:

The platform is designed as a robust **Batch Processing Pipeline**, ensuring data consistency, lineage, and auditability. It is **not** a streaming system; it operates on scheduled intervals (e.g., hourly, daily) to process massive datasets efficiently and cost-effectively.

### The Pipeline Flow
1.  **Scheduled Ingestion (EventBridge & Step Functions)**: Triggers the workflow on a defined schedule.
2.  **Partitioned Lakehouse (Iceberg)**: Data is stored in S3 time-partitioned (`date=DD-MM-YYYY`), enabling efficient backfills and temporal queries.
3.  **State Management (DynamoDB)**: Tracks checkpoints and run metadata, ensuring no data is missed or duplicated between batches.
4.  **Transformation (Glue/Spark)**: Heavy-lifting ETL jobs process entire partitions at once.

---

## 🚀 Key Features

### 1. Generic Ingestion Engine
The ingestion layer is decoupled from the business logic.
-   **Configurable Sources**: The Lambda-based ingestor fetches data based on configuration (Subreddits, Time Filters), not hardcoded rules.
-   **Raw Data Preservation**: All data is landed in a raw `Bronze` layer (S3), ensuring you can re-process history with new models later.

### 2. Pluggable Sentiment Analysis (The "Brain")
The logic for taking raw text and extracting value is modular.
-   **Showcase Module**: `modules/sentiment_analysis` contains the reference logic for Smartphones (Prompt Engineering for "Camera", "Battery", etc.).
-   **Adaptability**: By swapping the *Reference Data* (CSV) and the *Prompt Builder*, this same pipeline can analyze:
    -   **Cars**: (Engine, Comfort, Safety)
    -   **Games**: (Graphics, Gameplay, Story)
    -   **Hotels**: (Cleanliness, Service, Location)

### 3. "Medallion" Data Lakehouse
We use a multi-layer **Apache Iceberg** architecture to ensure data quality:
1.  **Landing Zone**: Raw JSONL from API.
2.  **Bronze**: Deduplicated, raw history.
3.  **Silver Core**: Cleaned, standardized data.
4.  **Silver Sentiment**: Enriched data with AI scores.
5.  **Gold (Snowflake)**: Business-ready Data Mart.

---

## ❄️ Gold Layer: Snowflake Integration

We bridge the gap between data engineering and business intelligence using **Snowflake** as the consumption layer.

-   **Zero-Copy Integration**: Snowflake reads the Silver Iceberg tables directly from S3 via **Glue Catalog Integration**. There is no "copy into" command; data is available instantly after the Glue job finishes.
-   **Star Schema Data Mart**:
    -   `fact_brand_mentions`: Central metrics table.
    -   `dim_product`: Domain-specific dimensions (replacable).
    -   `dim_date`: Time dimension.

---

## ⚡ Quick Start

### Prerequisites
-   **AWS Account** (Admin permissions)
-   **Terraform** (v1.0+) & **AWS CLI**
-   **Snowflake Account** (Optional, for Gold layer)

### 1. Build & Package
The pipeline uses Python-based Lambda functions and Glue scripts.

```bash
# 1. Build the Generic Ingestion Lambda
./scripts/build_lambda.sh

# 2. Build the Domain-Specific Sentiment Library
# (This packages the 'modules/sentiment_analysis' logic)
./scripts/build_glue_libs.sh
```

### 2. Deploy Infrastructure
Deploy the Terraform layers in order.

```bash
# Deploy S3 (Storage)
cd aws/us-east-1/10_s3 && terraform init && terraform apply -var-file=../../prod.tfvars

# Deploy Observability (DynamoDB)
cd ../11_dynamodb && terraform init && terraform apply -var-file=../../prod.tfvars

# Deploy Ingestion Engine
cd ../20_lambda && terraform init && terraform apply -var-file=../../prod.tfvars

# Deploy ETL Jobs (Bronze & Silver)
cd ../21_glue && terraform init && terraform apply -var-file=../../prod.tfvars
cd ../22_glue_enrichment && terraform init && terraform apply -var-file=../../prod.tfvars

# Deploy Orchestration (Step Functions & EventBridge)
cd ../30_stepfunction && terraform init && terraform apply -var-file=../../prod.tfvars
cd ../40_eventbridge && terraform init && terraform apply -var-file=../../prod.tfvars
```

---

## 📈 Observability & Monitoring

We believe in "Production-Grade" engineering.
-   **Batch Metrics**: Every batch run records its volume, duration, and data quality stats (e.g., "Usable Comment Ratio") to DynamoDB.
-   **Alerting**: Step Functions handles retries and failure notifications.

---

## 📜 Repository Structure

-   `aws/`: Terraform infrastructure code.
-   `lambda/`: **Generic** Ingestion Engine.
-   `modules/reddit_ingest/`: **Generic** Reddit API logic.
-   `modules/sentiment_analysis/`: **Domain-Specific** AI logic (Currently: Smartphones).
-   `snowflake_models/`: **Domain-Specific** SQL models.

---

## 🛡️ License

This project is licensed under the MIT License.
