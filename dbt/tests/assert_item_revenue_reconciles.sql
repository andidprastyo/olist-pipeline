select a.total_orders_fact, b.total_items_fact
from (select sum(items_revenue) as total_orders_fact from {{ ref('fct_orders') }}) a
cross join (select sum(price) as total_items_fact from {{ ref('fct_order_items') }}) b
where a.total_orders_fact <> b.total_items_fact