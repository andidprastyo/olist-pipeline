# olist-pipeline

A data pipeline that ingests the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle, converts the raw CSVs into partitioned Parquet files with DuckDB, loads them into an S3 data lake, then transforms them with dbt + DuckDB into a BI-ready star schema (staging -> marts).

## How it works

```
data/landing/*.csv          Raw CSVs downloaded from Kaggle
        │
        ▼   DuckDB (CSV → Parquet, all columns varchar + _loaded_at)
data/parquet/<table>/ingest_dt=YYYY-MM-DD/part-0.parquet
        │
        ▼   boto3
s3://$OLIST_BUCKET/raw/olist/<table>/ingest_dt=YYYY-MM-DD/part-0.parquet
        │
        ▼   dbt-duckdb (read_parquet on S3, hive partitioning)
dbt staging (cast + dedupe latest ingest_dt per key)
        │
        ▼
dbt marts (dim_* + fct_orders + fct_order_items)
        │
        ▼
warehouse/olist.duckdb
```

Each run ingests all 9 datasets (orders, order_items, order_payments, order_reviews, customers, products, sellers, geolocation, product_category_translation) partitioned by `ingest_dt`, making it safe to re-run for idempotent day-level loads. A run fails fast if any table produces zero rows.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- An S3 bucket for the landing zone
- Kaggle API credentials (`~/.kaggle/kaggle.json` or `KAGGLE_USERNAME`/`KAGGLE_KEY`)

## Setup

```bash
uv sync
export OLIST_BUCKET="your-bucket-name"
export AWS_REGION="us-east-1"
```

## Usage

```bash
# Ingest with today's date as ingest_dt
uv run ingest/ingest.py

# Ingest with an explicit partition date
uv run ingest/ingest.py 2026-10-18

# Build the warehouse: staging + marts + tests
cd dbt && dbt build --profiles-dir .
```

Output for each table:

```
orders: 99441 rows -> s3://your-bucket-name/raw/olist/orders/ingest_dt=2026-10-18/part-0.parquet
```

## Project layout

```
ingest/
  ingest.py            Ingestion: CSV → Parquet → S3
dbt/
  models/staging/      stg_* models: cast + dedupe by latest ingest_dt
  models/marts/        dim_customers, dim_products, dim_sellers, dim_date,
                       fct_orders, fct_order_items (+ schema.yml tests)
  tests/               reconciliation test (item revenue)
  macros/              custom schema naming (staging, marts)
  profiles.yml         DuckDB profile reading Parquet from S3
data/
  landing/             Raw CSVs (gitignored, not committed)
  parquet/             Local Parquet output organized by ingest_dt
warehouse/
  olist.duckdb         DuckDB warehouse (build target)
src/olist_pipeline/    Package entry point (CLI script: olist-pipeline)
```

## Roadmap

- [ ] Automated download from Kaggle
- [x] dbt-duckdb transformations on top of the raw layer (staging -> marts)
- [ ] Orchestration / scheduling for incremental daily loads