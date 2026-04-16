# Pharma Sales Analytics Pipeline

End-to-end data pipeline built on **Medallion Architecture**
(Bronze → Silver → Gold) processing 600K+ pharma sales records
across AWS, Snowflake, and Databricks.

## Architecture

![Architecture](docs/architecture.png)

## Tech Stack

| Layer       | Tool                        |
|-------------|-----------------------------|
| Storage     | AWS S3                      |
| ETL         | AWS Glue (PySpark)          |
| Warehouse   | Snowflake                   |
| Analytics   | Databricks                  |
| Language    | Python, SQL                 |

## Pipeline Layers

- **Bronze** — Raw CSV ingested from S3 with metadata columns
  (ingestion timestamp, source file) for data lineage
- **Silver** — Cleaned, type-cast, deduplicated Parquet files
  stored back in S3 across 8 drug category columns
- **Gold** — 3 aggregated business tables loaded into Snowflake:
  monthly drug sales, top drugs ranking, weekday trends
- **Analytics** — Databricks reads Gold tables via Python
  connector; 4 visualizations surface key business insights

## Key Insights Surfaced
- Top performing drug category by total units sold
- Monthly and seasonal sales trends over multiple years
- Year-over-year growth rate per drug category
- Highest sales days of the week across drug categories

## Project Structure

​```
├── aws/glue_jobs/
│   ├── bronze_job.py       # Raw ingestion + metadata
│   ├── silver_job.py       # Cleaning + transformation
│   └── gold_job.py         # Aggregations + unpivot
├── snowflake/
│   ├── setup.sql           # DDL for Gold tables
│   └── load_gold.sql       # COPY INTO scripts
├── databricks/
│   └── pharma_analytics.ipynb  # Analytics + visualizations
├── data/
│   └── sample/             # 50-row sample dataset
└── docs/
    └── architecture.png    # Pipeline architecture diagram
​```

## Setup Instructions

1. Upload `salesdaily.csv` to `s3://your-bucket/raw/`
2. Create IAM role `GluePharmaRole` with S3 and Glue permissions
3. Run Glue jobs in order:
   `bronze_job.py` → `silver_job.py` → `gold_job.py`
4. Run `snowflake/setup.sql` to create Gold tables
5. Run `snowflake/load_gold.sql` to load data from S3
6. Open `databricks/pharma_analytics.ipynb` and run all cells

## Dataset
[Pharma Sales Dataset](https://www.kaggle.com/datasets/milanzdravkovic/pharma-sales-data)
— 600K+ daily sales records across 8 drug categories (2014–2019)