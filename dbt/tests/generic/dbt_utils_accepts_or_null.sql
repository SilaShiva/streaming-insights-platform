{#
    Accepts a value OR null (standard dbt accepted_values fails on nulls).
    Values are compared as strings to sidestep type-casting in Jinja.
    Usage in yml:
      - dbt_utils_accepts_or_null:
          arguments:
            values: ['0']
#}
{% test dbt_utils_accepts_or_null(model, column_name, values) %}

with all_values as (
    select distinct cast({{ column_name }} as varchar) as value from {{ model }}
)
select *
from all_values
where value is not null
  and value not in ({% for v in values %}'{{ v }}'{% if not loop.last %},{% endif %}{% endfor %})

{% endtest %}
