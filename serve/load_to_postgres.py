import os
import duckdb

TABLES = ['fct_orders', 'fct_order_items', 'dim_customers', 'dim_products', 'dim_sellers', 'dim_date']

con = duckdb.connect('warehouse/olist.duckdb', read_only=True)
con.execute('INSTALL postgres; LOAD postgres;')
con.execute(f"ATTACH 'dbname=olist_bi host=127.0.0.1 user=bi password={os.environ['PG_PASSWORD']}' AS pg (TYPE postgres)")
con.execute('CREATE SCHEMA IF NOT EXISTS pg.marts')
for t in TABLES:
    con.execute(f'CREATE OR REPLACE TABLE pg.marts.{t} AS SELECT * FROM marts.{t}')
    print(t, con.execute(f'SELECT count(*) FROM pg.marts.{t}').fetchone()[0], 'rows published')