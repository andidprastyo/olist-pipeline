select
    review_id, order_id,
    cast(review_score as integer)              as review_score,
    cast(review_creation_date as timestamp)    as created_at,
    cast(review_answer_timestamp as timestamp) as answered_at
from {{ source('olist', 'order_reviews') }}
qualify row_number() over (partition by review_id, order_id order by ingest_dt desc) = 1