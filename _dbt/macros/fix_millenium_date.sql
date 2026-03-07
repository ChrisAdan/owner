{% macro fix_millennium_date(col) %}
    case
        when {{ col }}::varchar not in ('', '0001-01-01')
        then overlay({{ col }}::varchar placing '20' from 1 for 2)::date
    end
{% endmacro %}