{% macro hours_between(start_col, end_col) %}
    case
        when {{ start_col }} is not null and {{ end_col }} is not null
        then extract(epoch from ({{ end_col }} - {{ start_col }})) / 3600.0
    end
{% endmacro %}