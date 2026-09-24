\set QUIET on
\pset pager off
\set QUIET off
\echo 'Totales diarios: consume vs. carts crudos (revenue_diff debería ser ~0)'
with c as (
    select
        snapshot_date,
        sum((payload ->> 'discountedTotal')::numeric) as carts_net_total
    from raw.carts_snapshot
    group by snapshot_date
)

select
    m.date,
    c.carts_net_total,
    count(*) as products_sold,
    sum(m.units_sold) as units_sold,
    sum(m.revenue) as revenue,
    sum(m.revenue) - c.carts_net_total as revenue_diff
from consume.product_daily_revenue as m
inner join c on m.date = c.snapshot_date
group by m.date, c.carts_net_total
order by m.date;

\echo 'Auditoría de cargas'
select
    snapshot_date,
    resource,
    row_count,
    run_id,
    loaded_at
from raw.load_audit
order by loaded_at desc
limit 10;

\echo 'Calidad de datos de la última corrida (file = reglas que cumplen / reglas totales)'
with latest_run as (
    select run_id
    from raw.dq_results
    order by evaluated_at desc
    limit 1
)

select
    results.resource,
    results.rule,
    results.field,
    results.expected_accuracy,
    results.measured_accuracy,
    results.passed,
    results.failed,
    results.checked
from raw.dq_results as results
inner join latest_run on results.run_id = latest_run.run_id
order by
    results.resource asc,
    (results.rule = 'file') desc,
    results.passed asc,
    results.rule asc;
