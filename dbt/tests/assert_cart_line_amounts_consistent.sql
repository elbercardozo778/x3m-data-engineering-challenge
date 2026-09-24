-- bruto = precio x cantidad y neto = bruto x (1 - descuento), con tolerancia de redondeo a centavos.
select *
from {{ ref('stg_cart_lines') }}
where
    abs(gross_amount - unit_price * quantity) > 0.01
    or abs(net_amount - gross_amount * (1 - discount_percentage / 100)) > 0.02
