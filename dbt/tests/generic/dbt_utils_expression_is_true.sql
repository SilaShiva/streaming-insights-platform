{# Minimal local port of dbt_utils.expression_is_true (avoids the package dependency) #}
{% test dbt_utils_expression_is_true(model, column_name, expression) %}

select *
from {{ model }}
where not ({{ column_name }} {{ expression }})

{% endtest %}
