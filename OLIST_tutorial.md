# Olist ELT Tutorial: Kaggle CSVs to a BI-Ready Fact Table

*Minimal version. Stack: EC2, S3, Python, DuckDB, dbt. Optional: Postgres and Metabase for dashboards.*

```
Kaggle CSVs -> Python ingest -> S3 raw (Parquet) -> dbt + DuckDB (staging -> marts)
```

Placeholders used below: `<YOUR_BUCKET>` (your S3 bucket name) and `<REGION>` (for example `us-east-1`). Replace them with your own values. Never paste tokens, passwords or account IDs into code, chats, screenshots or GitHub.

## 1. AWS setup (console)

**S3 bucket.** Create a bucket with: Region `<REGION>` (same as your EC2 instance), General purpose, Global namespace, ACLs disabled, Block all public access ON (all four boxes), Versioning disabled, encryption SSE-S3. Leave everything else at its default.

**IAM policy.** IAM, Policies, Create policy, JSON tab. Paste this, replace `<YOUR_BUCKET>` in both places, and name it `olist-lake-access`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::<YOUR_BUCKET>"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::<YOUR_BUCKET>/*"
    }
  ]
}
```

**IAM role.** Roles, Create role, AWS service, use case EC2, attach `olist-lake-access`, name it `olist-ec2-role`. Then in EC2 select your instance, Actions, Security, Modify IAM role, choose `olist-ec2-role`, Update.

## 2. Project setup on EC2

Ubuntu 22.04 or 24.04, `t3.large` recommended, 30 GB disk. Create the folder first, then the virtual environment inside it:

```bash
sudo apt update && sudo apt install -y python3-venv python3-pip unzip git
mkdir -p ~/olist-pipeline && cd ~/olist-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install boto3 duckdb dbt-duckdb kaggle
mkdir -p data/landing data/parquet ingest warehouse dbt/models/staging dbt/models/marts dbt/macros dbt/tests
```

Your prompt should now start with `(.venv)`. The venv and environment variables last only for the current SSH session, so after every reconnect run:

```bash
cd ~/olist-pipeline && source .venv/bin/activate && set -a && source .env && set +a
```

Create the `.env` file (it is git-ignored later, never commit it):

```bash
cat > .env << 'EOF'
OLIST_BUCKET=<YOUR_BUCKET>
AWS_REGION=<REGION>
EOF
set -a; source .env; set +a
```

Check that the instance role works. The first command should print an ARN containing `olist-ec2-role`, the second should print `0` for an empty bucket:

```bash
python3 -c "import boto3; print(boto3.client('sts').get_caller_identity()['Arn'])"
python3 -c "import boto3,os; print(boto3.client('s3').list_objects_v2(Bucket=os.environ['OLIST_BUCKET']).get('KeyCount'))"
```

`AccessDenied` means the bucket name in the policy does not match. `Unable to locate credentials` means the role is not attached yet.

## 3. Download the data from Kaggle

On the Kaggle dataset page (Brazilian E-Commerce Public Dataset by Olist, slug `olistbr/brazilian-ecommerce`) accept the terms. In Kaggle Settings, API, create a token, then save it on the instance. Newer single-string token:

```bash
mkdir -p ~/.kaggle
nano ~/.kaggle/access_token    # paste the token, save
chmod 600 ~/.kaggle/access_token
```

Older `kaggle.json` token: paste the JSON into `~/.kaggle/kaggle.json` the same way and `chmod 600` it. Using `nano` instead of `echo` keeps the token out of your shell history. Then, from `~/olist-pipeline`:

```bash
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/landing --unzip
ls -lh data/landing
```

You should see 9 CSV files. If you get a 401, re-check the token file, or run `pip install -U kaggle`.

## 4. Ingest raw data to S3 as Parquet

Two rules: keep raw raw (every column read as a string, typing happens in dbt), and stay idempotent (the S3 key contains `ingest_dt`, so a re-run overwrites instead of duplicating). Create `ingest/ingest.py`:

```python
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


def main(ingest_dt):
    s3 = boto3.client('s3')
    con = duckdb.connect()
    for csv_name, table in TABLES.items():
        src = LANDING / csv_name
        target_dir = OUT / table / f'ingest_dt={ingest_dt}'
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / 'part-0.parquet'
        con.execute(f'''
            COPY (SELECT *, current_timestamp AS _loaded_at
                  FROM read_csv('{src}', header=true, all_varchar=true))
            TO '{target}' (FORMAT parquet, COMPRESSION zstd)
        ''')
        rows = con.execute(f'''SELECT count(*) FROM read_parquet('{target}')''').fetchone()[0]
        if rows == 0:
            raise ValueError(f'{table}: zero rows, refusing to upload')
        key = f'raw/olist/{table}/ingest_dt={ingest_dt}/part-0.parquet'
        s3.upload_file(str(target), BUCKET, key)
        print(f'{table}: {rows} rows -> s3://{BUCKET}/{key}')


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else dt.date.today().isoformat())
```

```bash
python3 ingest/ingest.py
```

Expected row counts (approx.): orders 99,441; order\_items 112,650; order\_payments 103,886; order\_reviews 99,224; customers 99,441; products 32,951; sellers 3,095; translation 71; geolocation about 1,000,000. If the reviews file fails to parse, add `quote='"', escape='"'` to the `read_csv` call.

## 5. dbt project files

All files in this section live under `~/olist-pipeline/dbt/`.

`dbt_project.yml`:

```yaml
name: olist
version: '1.0.0'
profile: olist
model-paths: ['models']
macro-paths: ['macros']
test-paths: ['tests']

models:
  olist:
    staging:
      +materialized: table
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

`profiles.yml` (S3 is read through the instance role, no keys needed):

```yaml
olist:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../warehouse/olist.duckdb
      schema: main
      extensions: [httpfs, parquet]
      settings:
        s3_region: "{{ env_var('AWS_REGION') }}"
      secrets:
        - type: s3
          provider: credential_chain
```

`macros/generate_schema_name.sql` (names schemas exactly `staging` and `marts`):

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}{{ target.schema }}
  {%- else -%}{{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
```

`models/staging/sources.yml` (`{name}` becomes the table name, `hive_partitioning` exposes `ingest_dt`):

```yaml
version: 2

sources:
  - name: olist
    meta:
      external_location: "read_parquet('s3://{{ env_var('OLIST_BUCKET') }}/raw/olist/{name}/*/*.parquet', hive_partitioning=true)"
    tables:
      - name: orders
      - name: order_items
      - name: order_payments
      - name: order_reviews
      - name: customers
      - name: products
      - name: sellers
      - name: product_category_translation
```

Check the connection (a first run may download DuckDB extensions, which needs internet access):

```bash
cd ~/olist-pipeline/dbt && dbt debug --profiles-dir .
```

## 6. Staging models

In `models/staging/`. Each model renames columns, casts types, and keeps only the latest ingested row per business key (the `qualify` line), which makes the pipeline safe to re-run.

`stg_orders.sql`

```sql
select
    order_id, customer_id, order_status,
    cast(order_purchase_timestamp as timestamp)      as purchased_at,
    cast(order_approved_at as timestamp)             as approved_at,
    cast(order_delivered_carrier_date as timestamp)  as delivered_to_carrier_at,
    cast(order_delivered_customer_date as timestamp) as delivered_to_customer_at,
    cast(order_estimated_delivery_date as timestamp) as estimated_delivery_at
from {{ source('olist', 'orders') }}
qualify row_number() over (partition by order_id order by ingest_dt desc) = 1
```

`stg_order_items.sql`

```sql
select
    order_id,
    cast(order_item_id as integer)         as order_item_id,
    product_id, seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_at,
    cast(price as decimal(12, 2))          as price,
    cast(freight_value as decimal(12, 2))  as freight_value
from {{ source('olist', 'order_items') }}
qualify row_number() over (partition by order_id, order_item_id order by ingest_dt desc) = 1
```

`stg_payments.sql`

```sql
select
    order_id,
    cast(payment_sequential as integer)   as payment_sequential,
    payment_type,
    cast(payment_installments as integer) as payment_installments,
    cast(payment_value as decimal(12, 2)) as payment_value
from {{ source('olist', 'order_payments') }}
qualify row_number() over (partition by order_id, payment_sequential order by ingest_dt desc) = 1
```

`stg_reviews.sql` (the raw file repeats `review_id`, so dedupe on the pair; free-text comments are dropped)

```sql
select
    review_id, order_id,
    cast(review_score as integer)              as review_score,
    cast(review_creation_date as timestamp)    as created_at,
    cast(review_answer_timestamp as timestamp) as answered_at
from {{ source('olist', 'order_reviews') }}
qualify row_number() over (partition by review_id, order_id order by ingest_dt desc) = 1
```

`stg_customers.sql`

```sql
select
    customer_id, customer_unique_id,
    customer_zip_code_prefix as zip_code_prefix,
    customer_city            as city,
    customer_state           as state
from {{ source('olist', 'customers') }}
qualify row_number() over (partition by customer_id order by ingest_dt desc) = 1
```

`stg_sellers.sql`

```sql
select
    seller_id,
    seller_zip_code_prefix as zip_code_prefix,
    seller_city            as city,
    seller_state           as state
from {{ source('olist', 'sellers') }}
qualify row_number() over (partition by seller_id order by ingest_dt desc) = 1
```

`stg_products.sql` (adds the English category name)

```sql
select
    p.product_id,
    coalesce(t.product_category_name_english, p.product_category_name, 'unknown') as category_name,
    cast(p.product_photos_qty as integer) as photos_qty,
    cast(p.product_weight_g as integer)   as weight_g,
    cast(p.product_length_cm as integer)  as length_cm,
    cast(p.product_height_cm as integer)  as height_cm,
    cast(p.product_width_cm as integer)   as width_cm
from (
    select * from {{ source('olist', 'products') }}
    qualify row_number() over (partition by product_id order by ingest_dt desc) = 1
) p
left join (
    select * from {{ source('olist', 'product_category_translation') }}
    qualify row_number() over (partition by product_category_name order by ingest_dt desc) = 1
) t on p.product_category_name = t.product_category_name
```

## 7. Marts (dimensions and facts)

In `models/marts/`.

`dim_customers.sql` (`customer_id` is per order; `customer_unique_id` identifies the person)

```sql
select customer_id, customer_unique_id, zip_code_prefix, city, state
from {{ ref('stg_customers') }}
```

`dim_products.sql`

```sql
select * from {{ ref('stg_products') }}
```

`dim_sellers.sql`

```sql
select * from {{ ref('stg_sellers') }}
```

`dim_date.sql`

```sql
select
    cast(d as date)        as date_key,
    year(d)                as year,
    quarter(d)             as quarter,
    month(d)               as month,
    strftime(d, '%Y-%m')   as year_month,
    monthname(d)           as month_name,
    isodow(d)              as day_of_week,
    dayname(d)             as day_name,
    isodow(d) in (6, 7)    as is_weekend
from generate_series(timestamp '2016-01-01', timestamp '2018-12-31', interval 1 day) as t(d)
```

`fct_order_items.sql` (**grain: one row per order item**, for product, seller and category analysis)

```sql
select
    i.order_id, i.order_item_id, i.product_id, i.seller_id, o.customer_id,
    cast(o.purchased_at as date) as purchase_date_key,
    o.order_status,
    i.price, i.freight_value,
    i.price + i.freight_value    as total_item_value
from {{ ref('stg_order_items') }} i
inner join {{ ref('stg_orders') }} o on i.order_id = o.order_id
```

`fct_orders.sql` (**grain: one row per order**). Items, payments and reviews are each aggregated to one row per order *before* joining. Joining them raw multiplies rows and inflates revenue.

```sql
with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select customer_id, customer_unique_id from {{ ref('stg_customers') }}
),
items as (
    select order_id,
           count(*)                  as item_count,
           count(distinct seller_id) as seller_count,
           sum(price)                as items_revenue,
           sum(freight_value)        as freight_total
    from {{ ref('stg_order_items') }}
    group by 1
),
payments as (
    select order_id,
           sum(payment_value)                   as payment_total,
           max(payment_installments)            as max_installments,
           arg_max(payment_type, payment_value) as main_payment_type
    from {{ ref('stg_payments') }}
    group by 1
),
reviews as (
    select order_id, review_score
    from {{ ref('stg_reviews') }}
    qualify row_number() over (partition by order_id order by answered_at desc nulls last) = 1
)
select
    o.order_id, o.customer_id, c.customer_unique_id,
    cast(o.purchased_at as date)  as purchase_date_key,
    o.order_status,
    o.order_status = 'delivered'  as is_delivered,
    o.purchased_at, o.approved_at, o.delivered_to_carrier_at,
    o.delivered_to_customer_at, o.estimated_delivery_at,
    coalesce(i.item_count, 0)     as item_count,
    coalesce(i.seller_count, 0)   as seller_count,
    coalesce(i.items_revenue, 0)  as items_revenue,
    coalesce(i.freight_total, 0)  as freight_total,
    coalesce(p.payment_total, 0)  as payment_total,
    p.max_installments, p.main_payment_type,
    date_diff('day', o.purchased_at, o.delivered_to_customer_at)          as delivery_days,
    date_diff('day', o.purchased_at, o.estimated_delivery_at)             as estimated_delivery_days,
    date_diff('day', o.estimated_delivery_at, o.delivered_to_customer_at) as delivery_delay_days,
    o.delivered_to_customer_at > o.estimated_delivery_at                  as is_late,
    r.review_score
from orders o
left join customers c on o.customer_id = c.customer_id
left join items     i on o.order_id = i.order_id
left join payments  p on o.order_id = p.order_id
left join reviews   r on o.order_id = r.order_id
```

`is_late` stays NULL for undelivered orders (unknown, not false). `payment_total` can differ slightly from items plus freight because of vouchers and installment interest.

## 8. Tests

`models/marts/schema.yml`

```yaml
version: 2

models:
  - name: fct_orders
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: customer_id
        tests:
          - not_null
          - relationships: {to: ref('dim_customers'), field: customer_id}
      - name: purchase_date_key
        tests:
          - not_null
          - relationships: {to: ref('dim_date'), field: date_key}
      - name: review_score
        tests:
          - accepted_values: {values: [1, 2, 3, 4, 5], quote: false}

  - name: fct_order_items
    columns:
      - name: order_id
        tests:
          - not_null
          - relationships: {to: ref('fct_orders'), field: order_id}
      - name: product_id
        tests:
          - relationships: {to: ref('dim_products'), field: product_id}
      - name: seller_id
        tests:
          - relationships: {to: ref('dim_sellers'), field: seller_id}

  - name: dim_customers
    columns:
      - name: customer_id
        tests: [unique, not_null]
  - name: dim_products
    columns:
      - name: product_id
        tests: [unique, not_null]
  - name: dim_sellers
    columns:
      - name: seller_id
        tests: [unique, not_null]
  - name: dim_date
    columns:
      - name: date_key
        tests: [unique, not_null]
```

`tests/assert_item_revenue_reconciles.sql` (passes when it returns zero rows; catches silent join errors)

```sql
select a.total_orders_fact, b.total_items_fact
from (select sum(items_revenue) as total_orders_fact from {{ ref('fct_orders') }}) a
cross join (select sum(price) as total_items_fact from {{ ref('fct_order_items') }}) b
where a.total_orders_fact <> b.total_items_fact
```

## 9. Run and verify

```bash
cd ~/olist-pipeline/dbt && dbt build --profiles-dir .
```

`dbt build` runs models and tests in dependency order, and a failing test skips everything downstream. Then check the result (DuckDB allows one writer, so close other processes using the file first):

```bash
cd ~/olist-pipeline
python3 - << 'EOF'
import duckdb
con = duckdb.connect('warehouse/olist.duckdb', read_only=True)
print(con.sql('select count(*) as orders, sum(items_revenue) as revenue from marts.fct_orders'))
print(con.sql('select count(*) as items from marts.fct_order_items'))
print(con.sql('select is_late, count(*) n, round(avg(review_score), 2) avg_review from marts.fct_orders where is_delivered group by 1'))
EOF
```

Expect about 99k orders, 112k items, and about 13.6 million BRL in item revenue. Late orders should show a clearly lower average review score, which is a good headline insight for a dashboard.

## 10. Push to GitHub (without the data)

Create an empty repo on GitHub first (no README, no .gitignore, no license). Then, from `~/olist-pipeline`, **before the first commit**:

```bash
cat >> .gitignore << 'EOF'
.env
.venv/
data/
warehouse/
dbt/target/
dbt/logs/
dbt/dbt_packages/
*.duckdb
*.parquet
*.csv
*.log
__pycache__/
EOF

git init -b main
git add .
git status
```

Read the `git status` output: `.env`, `data/` and `.venv/` must not be listed. Then:

```bash
git commit -m "Add Olist pipeline"
git remote add origin https://github.com/<your-username>/olist-pipeline.git
git push -u origin main
```

GitHub does not accept your account password. When prompted, enter your username and a personal access token (GitHub, Settings, Developer settings, Personal access tokens) with `repo` scope. "Repository not found" means the repo name is wrong or the token cannot access it. If data files were already committed but not pushed, run `git rm -r --cached data/` and `git commit --amend` before pushing. If a secret ever reaches a commit, revoke and replace it.

## 11. Optional: serve the marts to a BI tool

DuckDB is single-writer, so publish the finished marts into Postgres and point your BI tool at that. Replace `<DB_PASSWORD>` with your own value, and keep it in `.env`, not in code:

```bash
echo 'PG_PASSWORD=<DB_PASSWORD>' >> .env && set -a && source .env && set +a
sudo apt install -y docker.io && sudo usermod -aG docker $USER   # log out and back in afterwards
docker run -d --name bi-postgres --restart unless-stopped -e POSTGRES_USER=bi -e POSTGRES_PASSWORD=$PG_PASSWORD -e POSTGRES_DB=olist_bi -p 127.0.0.1:5432:5432 -v pgdata:/var/lib/postgresql/data postgres:16
```

`serve/load_to_postgres.py` (create the `serve/` folder first):

```python
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
```

```bash
python3 serve/load_to_postgres.py
docker run -d --name metabase --restart unless-stopped --network host metabase/metabase
```

Metabase listens on port 3000. Do not open that port in the security group. From your laptop use an SSH tunnel and browse to `http://localhost:3000`:

```bash
ssh -i <your-key>.pem -L 3000:localhost:3000 ubuntu@<ec2-public-ip>
```

In Metabase add a PostgreSQL database: host `localhost`, port `5432`, database `olist_bi`, user `bi`.

## Troubleshooting

| Symptom | Likely fix |
| --- | --- |
| `No module named boto3` or `duckdb` | The venv is not active. Run the activation line from Section 2. |
| `KeyError: OLIST_BUCKET` or dbt cannot find an env var | `.env` was not loaded in this session. Run `set -a; source .env; set +a`. |
| dbt cannot read S3 | The role is not attached or lacks `s3:ListBucket`, or `AWS_REGION` is wrong. Re-run the checks in Section 2. |
| `Could not set lock on file` | Another process holds the DuckDB file. Stop it, or open with `read_only=True`. |
| `fct_orders` revenue too high | A join fan-out. Aggregate items, payments and reviews to one row per order before joining. |
| `git push` fails with auth error or not found | Use a personal access token as the password, and check the repo URL with `git remote -v`. |

## Next steps

Replay the data as daily batches (one `ingest_dt` per day), schedule the three stages (ingest, `dbt build`, publish) with cron or Airflow, add a GitHub Actions workflow that runs `dbt build`, and define the bucket, role and instance in Terraform.
