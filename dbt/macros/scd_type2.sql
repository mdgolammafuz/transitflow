{% macro scd_type2_merge(target_table, source_table, natural_key, tracked_columns) %}

-- Use a stable timestamp for the entire transaction.
-- Hardened: Using current_timestamp (which is UTC-locked via profiles.yml) 
-- to ensure valid_from/valid_to are perfectly aligned with telemetry timestamps.
{% set execution_time = "current_timestamp::timestamp" %}

-- 1. Close existing records
update {{ target_table }} as t
set 
    valid_to = {{ execution_time }}, 
    is_current = false
from {{ source_table }} as s
where t.{{ natural_key }} = s.{{ natural_key }}
  and t.is_current = true
  and (
    {% for col in tracked_columns %}
    coalesce(t.{{ col }}::text, '') != coalesce(s.{{ col }}::text, ''){% if not loop.last %} or {% endif %}
    {% endfor %}
  );

-- 2. Insert new records
insert into {{ target_table }} (
    {{ natural_key }}_key,
    {% for col in tracked_columns %} {{ col }}, {% endfor %}
    {{ natural_key }},
    valid_from,
    valid_to,
    is_current
)
select
    -- Surrogate key includes the execution time to ensure uniqueness per SCD grain
    {{ dbt_utils.generate_surrogate_key([natural_key, execution_time]) }} as {{ natural_key }}_key,
    {% for col in tracked_columns %} s.{{ col }}, {% endfor %}
    s.{{ natural_key }},
    {{ execution_time }} as valid_from,
    cast('{{ var("scd_end_date") }}' as timestamp) as valid_to,
    true as is_current
from {{ source_table }} as s;

{% endmacro %}