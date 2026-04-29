{% macro safe_cast(column_expr, target_type, column_alias='') %}
    TRY_CAST({{ column_expr }} AS {{ target_type }})
{% endmacro %}