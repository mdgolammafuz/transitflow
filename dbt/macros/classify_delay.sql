{% macro classify_delay(column_name) %}
    case
        when abs({{ column_name }}) <= {{ var('on_time_threshold') }} then 'on_time'
        when {{ column_name }} > {{ var('delayed_threshold') }} then 'delayed'
        when {{ column_name }} < -{{ var('delayed_threshold') }} then 'early'
        else 'slight_delay'
    end
{% endmacro %}