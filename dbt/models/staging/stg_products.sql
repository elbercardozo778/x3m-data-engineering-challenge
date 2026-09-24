select
    snapshot_date,
    id as product_id,
    payload ->> 'title' as product_title,
    payload ->> 'category' as category,
    payload ->> 'brand' as brand,
    (payload ->> 'price')::numeric(12, 2) as price,
    (payload ->> 'discountPercentage')::numeric as discount_percentage,
    (payload ->> 'stock')::integer as stock
from {{ source('raw', 'products_snapshot') }}
{{ snapshot_date_filter('snapshot_date') }}
