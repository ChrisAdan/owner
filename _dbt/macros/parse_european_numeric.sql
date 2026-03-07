{% macro parse_european_numeric(col, precision=12, scale=2) %}
    case
        when {{ col }} is not null and trim({{ col }}) != ''
        then replace(
            regexp_replace(trim({{ col }}), '[^0-9,]', '', 'g'),
            ',', '.'
        )::numeric({{ precision }}, {{ scale }})
    end
{% endmacro %}