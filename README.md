# olist-pipeline

A data pipeline that ingests the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle, converts the raw CSVs into partitioned Parquet files with DuckDB, and loads them into an S3 data lake.

## How it works

```
data/landing/*.csv          Raw CSVs downloaded from Kaggle
        │
        ▼   DuckDB (CSV → Parquet, all columns varchar + _loaded_at)
data/parquet/<table>/ingest_dt=YYYY-MM-DD/part-0.parquet
        │
        ▼   boto3
s3://$OLIST_BUCKET/raw/olist/<table>/ingest_dt=YYYY-MM-DD/part-0.parquet
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
```

## Usage

```bash
# Ingest with today's date as ingest_dt
uv run ingest/ingest.py

# Ingest with an explicit partition date
uv run ingest/ingest.py 2026-10-18
```

Output for each table:

```
orders: 99441 rows -> s3://your-bucket-name/raw/olist/orders/ingest_dt=2026-10-18/part-0.parquet
```

## Project layout

```
ingest/
  ingest.py            Ingestion: CSV → Parquet → S3
data/
  landing/             Raw CSVs (gitignored, not committed)
  parquet/             Local Parquet output organized by ingest_dt
src/olist_pipeline/    Package entry point (CLI script: olist-pipeline)
```

## Roadmap

- [ ] Automated download from Kaggle
- [ ] dbt-duckdb transformations on top of the raw layer (already a dependency)
- [ ] Orchestration / scheduling for incremental daily loads
