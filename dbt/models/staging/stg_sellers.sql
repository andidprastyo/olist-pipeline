select
    seller_id,
    seller_zip_code_prefix as zip_code_prefix,
    seller_city            as city,
    seller_state           as state
from {{ source('olist', 'sellers') }}
qualify row_number() over (partition by seller_id order by ingest_dt desc) = 1