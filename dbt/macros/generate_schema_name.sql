{#
    Pin custom schemas to their literal names ("staging", "marts") instead of
    dbt's default <target_schema>_<custom> prefixing. Keeps model locations
    deterministic across environments (local DuckDB, CI, BigQuery) so
    serving-layer queries and tests don't depend on the target schema name.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
