-- Un cart puede repetir el mismo producto en varias líneas, por eso la posición de la línea es parte de la clave.
select
    carts.snapshot_date,
    carts.id as cart_id,
    line.line_number::integer as line_number,
    (line.item ->> 'id')::integer as product_id,
    line.item ->> 'title' as product_title,
    (line.item ->> 'price')::numeric(12, 2) as unit_price,
    (line.item ->> 'quantity')::integer as quantity,
    (line.item ->> 'discountPercentage')::numeric as discount_percentage,
    (line.item ->> 'total')::numeric(14, 2) as gross_amount,
    (line.item ->> 'discountedTotal')::numeric(14, 2) as net_amount
from {{ source('raw', 'carts_snapshot') }} as carts
cross join
    lateral jsonb_array_elements(carts.payload -> 'products')
    with ordinality as line (item, line_number)
{{ snapshot_date_filter('carts.snapshot_date') }}
