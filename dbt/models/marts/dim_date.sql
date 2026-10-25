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