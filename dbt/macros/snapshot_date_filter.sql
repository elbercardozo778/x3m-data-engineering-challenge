{#- En corridas incrementales procesa solo el snapshot de la corrida; en el primer build o con --full-refresh, todo. -#}
{% macro snapshot_date_filter(column) -%}
    {%- if is_incremental() and var('snapshot_date', none) -%}
        where {{ column }} = '{{ var("snapshot_date") }}'::date
    {%- endif -%}
{%- endmacro %}
