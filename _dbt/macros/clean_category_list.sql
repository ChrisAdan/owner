{% macro clean_category_list(col) %}
    nullif(
        regexp_replace(trim({{ col }}), '[\\[\\]'' ]', '', 'g'),
        ''
    )
{% endmacro %}