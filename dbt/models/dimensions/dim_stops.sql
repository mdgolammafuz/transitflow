/*
    Dimension: Stops (SCD Type 2)
    
    Pattern: Slowly Changing Dimension Type 2
    - Tracks historical changes to stop attributes
    - Creates new row when stop_name or location changes
    - Preserves full history with valid_from/valid_to dates
    
    Interview talking point:
    "When a stop is renamed, we don't update in place. We close the old 
    record (set valid_to) and insert a new record (valid_from = today).
    This preserves history so we can answer 'what was this stop called
    on date X?' - crucial for historical analysis and ML training."
*/

with seed_stops as (
    select * from {{ ref('seed_stops') }}
),

-- Current state from seed data
current_stops as (
    select
        stop_id,
        stop_name,
        stop_lat as latitude,
        stop_lon as longitude,
        zone_id,
        stop_code,
        -- SCD Type 2 fields
        current_date as valid_from,
        cast('{{ var("scd_end_date") }}' as date) as valid_to,
        true as is_current
    from seed_stops
),

-- Generate surrogate key
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['stop_id', 'valid_from']) }} as stop_key,
        stop_id,
        stop_name,
        latitude,
        longitude,
        zone_id,
        stop_code,
        valid_from,
        valid_to,
        is_current
    from current_stops
)

select * from final