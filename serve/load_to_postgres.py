import os
import pathlib
import duckdb

TABLES = ['fct_orders', 'fct_order_items', 'dim_customers', 'dim_products', 'dim_sellers', 'dim_date']

DB = pathlib.Path('warehouse/olist.duckdb')
if not DB.exists():
    raise SystemExit(f'{DB} not found. Build the warehouse first: cd dbt && dbt build --profiles-dir .')

con = duckdb.connect(str(DB), read_only=True)

present = {r[0] for r in con.execute(
    "select table_name from information_schema.tables where table_schema = 'marts'"
).fetchall()}
missing = [t for t in TABLES if t not in present]
if missing:
    raise SystemExit(f'marts.{", marts.".join(missing)} not built. Run: cd dbt && dbt build --profiles-dir .')

con.execute('INSTALL postgres; LOAD postgres;')
con.execute(f"ATTACH 'dbname=olist_bi host=127.0.0.1 user=bi password={os.environ['PG_PASSWORD']}' AS pg (TYPE postgres, READ_ONLY false)")
con.execute('CREATE SCHEMA IF NOT EXISTS pg.marts')
for t in TABLES:
    con.execute(f'CREATE OR REPLACE TABLE pg.marts.{t} AS SELECT * FROM marts.{t}')
    print(t, con.execute(f'SELECT count(*) FROM pg.marts.{t}').fetchone()[0], 'rows published')