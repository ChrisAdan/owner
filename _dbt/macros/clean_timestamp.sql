{% macro clean_timestamp(col) %}
    nullif(trim({{ col }}::varchar), '')::timestamp
{% endmacro %}