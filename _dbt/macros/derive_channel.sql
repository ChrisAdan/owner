{% macro derive_channel(form_submission_date_col) %}
    case
        when {{ form_submission_date_col }} is not null then 'inbound'
        else 'outbound'
    end
{% endmacro %}