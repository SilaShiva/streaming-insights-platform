{#
    Accepts a value OR null (standard dbt accepted_values fails on nulls).
    Usage in yml:
      - dbt_utils_accepts_or_null:
          arguments:
            values: [0]
#}
{% test dbt_utils_accepts_or_null(model, column_name, values) %}

with all_values as (
    select distinct {{ column_name }} as value from {{ model }}
)
select *
from all_values
where value is not null
  and value not in (
    select cast(v as {{ dbt_utils_type(column_name) }})
    from (values {% for v in values %}({{ v }}){% if not loop.last %},{% endif %}{% endfor %}) as t(v)
  )

{% endtest %}

{% macro dbt_utils_type(column_name) %}
    {{ return('integer') }}
{% endmacro %}
