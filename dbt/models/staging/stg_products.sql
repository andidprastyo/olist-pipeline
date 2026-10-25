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