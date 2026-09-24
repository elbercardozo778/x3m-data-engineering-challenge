-- Solo tienen fila los productos con al menos una venta ese día.
with lines as (
    select *
    from {{ ref('stg_cart_lines') }}
    {{ snapshot_date_filter('snapshot_date') }}
),

daily as (
    select
        snapshot_date,
        product_id,
        sum(quantity) as units_sold,
        sum(net_amount) as revenue,
        sum(gross_amount) as gross_revenue,
        count(distinct cart_id) as carts_count
    from lines
    group by snapshot_date, product_id
)

select
    daily.product_id,
    products.product_title,
    daily.snapshot_date as date,
    daily.units_sold,
    daily.revenue,
    daily.gross_revenue,
    daily.carts_count
from daily
left join {{ ref('stg_products') }} as products
    on
        daily.snapshot_date = products.snapshot_date
        and daily.product_id = products.product_id
