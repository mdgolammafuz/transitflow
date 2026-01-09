/*
    Macro for SCD Type 2 dimension handling.
    
    Pattern: Slowly Changing Dimension Type 2
    - Creates new row when attributes change
    - Closes previous row (sets valid_to)
    - Preserves full history
    
    Usage:
    {{ scd_type2_merge(
        target_table='dim_stops',
        source_table='stg_stops',
        natural_key='stop_id',
        tracked_columns=['stop_name', 'latitude', 'longitude']
    ) }}
*/

{% macro scd_type2_merge(target_table, source_table, natural_key, tracked_columns) %}

-- Close existing records where tracked columns have changed
update {{ target_table }} as t
set 
    valid_to = current_date - interval '1 day',
    is_current = false
from {{ source_table }} as s
where t.{{ natural_key }} = s.{{ natural_key }}
  and t.is_current = true
  and (
    {% for col in tracked_columns %}
    t.{{ col }} != s.{{ col }}{% if not loop.last %} or {% endif %}
    {% endfor %}
  );

-- Insert new records for changed or new items
insert into {{ target_table }}
select
    {{ dbt_utils.generate_surrogate_key([natural_key, 'current_date']) }} as {{ natural_key }}_key,
    s.*,
    current_date as valid_from,
    cast('9999-12-31' as date) as valid_to,
    true as is_current
from {{ source_table }} as s
left join {{ target_table }} as t
    on s.{{ natural_key }} = t.{{ natural_key }}
    and t.is_current = true
where t.{{ natural_key }} is null  -- New records
   or (
    {% for col in tracked_columns %}
    t.{{ col }} != s.{{ col }}{% if not loop.last %} or {% endif %}
    {% endfor %}
   );  -- Changed records

{% endmacro %}