{% macro classify_delay(column_name) %}
    case
        -- ALIGNED: Matches dbt_project.yml var names
        when abs({{ column_name }}) <= {{ var('on_time_threshold_seconds') }} then 'on_time'
        when {{ column_name }} > {{ var('late_threshold_seconds') }} then 'delayed'
        when {{ column_name }} < -{{ var('late_threshold_seconds') }} then 'early'
        else 'slight_delay'
    end
{% endmacro %}