{#- Usa el schema tal cual (staging, consume) en vez del prefijo "<target>_<custom>" que dbt pone por defecto. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
