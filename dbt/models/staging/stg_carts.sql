select
    snapshot_date,
    id as cart_id,
    (payload ->> 'userId')::integer as user_id,
    (payload ->> 'totalProducts')::integer as total_products,
    (payload ->> 'totalQuantity')::integer as total_quantity,
    (payload ->> 'total')::numeric(14, 2) as gross_total,
    (payload ->> 'discountedTotal')::numeric(14, 2) as net_total
from {{ source('raw', 'carts_snapshot') }}
{{ snapshot_date_filter('snapshot_date') }}
