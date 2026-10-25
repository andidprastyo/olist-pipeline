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