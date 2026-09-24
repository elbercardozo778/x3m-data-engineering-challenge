select
    carts.snapshot_date,
    carts.cart_id,
    carts.net_total,
    sum(cart_lines.net_amount) as lines_net
from {{ ref('stg_carts') }} as carts
inner join {{ ref('stg_cart_lines') }} as cart_lines
    on
        carts.snapshot_date = cart_lines.snapshot_date
        and carts.cart_id = cart_lines.cart_id
group by carts.snapshot_date, carts.cart_id, carts.net_total
having abs(carts.net_total - sum(cart_lines.net_amount)) > 0.05
