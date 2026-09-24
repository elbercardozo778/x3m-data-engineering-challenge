\set QUIET on
\pset pager off
\set QUIET off
\echo 'product_daily_revenue: resultado del último snapshot, ordenado por revenue'
select
    pdr.product_id,
    pdr.product_title,
    pdr.date,
    pdr.units_sold,
    pdr.revenue,
    pdr.gross_revenue
from consume.product_daily_revenue as pdr
where pdr.date = (select max(latest.date) from consume.product_daily_revenue as latest)
order by pdr.revenue desc, pdr.product_id asc;
