select
    i.order_id, i.order_item_id, i.product_id, i.seller_id, o.customer_id,
    cast(o.purchased_at as date) as purchase_date_key,
    o.order_status,
    i.price, i.freight_value,
    i.price + i.freight_value    as total_item_value
from {{ ref('stg_order_items') }} i
inner join {{ ref('stg_orders') }} o on i.order_id = o.order_id