-- La agregación no puede perder ni duplicar plata: el revenue del día es igual a la suma neta de los carts.
with mart as (
    select
        date,
        sum(revenue) as revenue
    from {{ ref('product_daily_revenue') }}
    group by date
),

carts as (
    select
        snapshot_date as date,
        sum(net_total) as revenue
    from {{ ref('stg_carts') }}
    group by snapshot_date
)

select
    mart.date,
    mart.revenue as mart_revenue,
    carts.revenue as carts_revenue
from mart
full outer join carts on mart.date = carts.date
where mart.date is null or carts.date is null or abs(mart.revenue - carts.revenue) > 1
