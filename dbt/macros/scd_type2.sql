/*
    Macro for SCD Type 2 dimension handling.
    Pattern: Slowly Changing Dimension Type 2 (Manual Implementation)
    Harden: Uses project-level variables and ensures timestamp precision.
*/

{% macro scd_type2_merge(target_table, source_table, natural_key, tracked_columns) %}

-- 1. Close existing records where tracked columns have changed
update {{ target_table }} as t
set 
    valid_to = current_timestamp,
    is_current = false
from {{ source_table }} as s
where t.{{ natural_key }} = s.{{ natural_key }}
  and t.is_current = true
  and (
    {% for col in tracked_columns %}
    coalesce(t.{{ col }}::text, '') != coalesce(s.{{ col }}::text, ''){% if not loop.last %} or {% endif %}
    {% endfor %}
  );

-- 2. Insert new records for changed or new items
insert into {{ target_table }}
select
    -- Surrogate key includes timestamp to ensure uniqueness during re-runs
    {{ dbt_utils.generate_surrogate_key([natural_key, 'current_timestamp']) }} as {{ natural_key }}_key,
    s.*,
    current_timestamp as valid_from,
    cast('{{ var("scd_end_date") }}' as timestamp) as valid_to,
    true as is_current
from {{ source_table }} as s
left join {{ target_table }} as t
    on s.{{ natural_key }} = t.{{ natural_key }}
    and t.is_current = true
where t.{{ natural_key }} is null  -- New records
   or (
    {% for col in tracked_columns %}
    coalesce(t.{{ col }}::text, '') != coalesce(s.{{ col }}::text, ''){% if not loop.last %} or {% endif %}
    {% endfor %}
   );  -- Changed records

{% endmacro %}