import os
import sys
import pathlib
import datetime as dt

import boto3
import duckdb

BUCKET = os.environ['OLIST_BUCKET']
LANDING = pathlib.Path('data/landing')
OUT = pathlib.Path('data/parquet')

TABLES = {
    'olist_orders_dataset.csv': 'orders',
    'olist_order_items_dataset.csv': 'order_items',
    'olist_order_payments_dataset.csv': 'order_payments',
    'olist_order_reviews_dataset.csv': 'order_reviews',
    'olist_customers_dataset.csv': 'customers',
    'olist_products_dataset.csv': 'products',
    'olist_sellers_dataset.csv': 'sellers',
    'olist_geolocation_dataset.csv': 'geolocation',
    'product_category_name_translation.csv': 'product_category_translation',
}


def main(ingest_dt: str) -> None:
    s3 = boto3.client('s3')
    con = duckdb.connect()

    for csv_name, table in TABLES.items():
        src = LANDING / csv_name
        target_dir = OUT / table / f'ingest_dt={ingest_dt}'
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / 'part-0.parquet'

        con.execute(f'''
            COPY (
                SELECT *, current_timestamp AS _loaded_at
                FROM read_csv('{src}', header=true, all_varchar=true)
            ) TO '{target}' (FORMAT parquet, COMPRESSION zstd)
        ''')

        rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
        if rows == 0:
            raise ValueError(f'{table}: zero rows, refusing to upload')

        key = f'raw/olist/{table}/ingest_dt={ingest_dt}/part-0.parquet'
        s3.upload_file(str(target), BUCKET, key)
        print(f'{table}: {rows} rows -> s3://{BUCKET}/{key}')


if __name__ == '__main__':
    run_date = sys.argv[1] if len(sys.argv) > 1 else dt.date.today().isoformat()
    main(run_date)
