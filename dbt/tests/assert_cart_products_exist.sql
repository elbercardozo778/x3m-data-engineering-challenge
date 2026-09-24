-- Todo producto vendido tiene que existir en el snapshot de productos del mismo día.
select cart_lines.*
from {{ ref('stg_cart_lines') }} as cart_lines
left join {{ ref('stg_products') }} as products
    on
        cart_lines.snapshot_date = products.snapshot_date
        and cart_lines.product_id = products.product_id
where products.product_id is null
