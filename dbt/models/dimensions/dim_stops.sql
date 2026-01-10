/*
    Dimension: Stops (SCD Type 2)
    Pattern: Slowly Changing Dimension Type 2
    
    This model transforms the raw snapshot history into a clean dimension.
*/

with snapshot_source as (
    -- Pulling from the snapshot we created in the dbt/snapshots/ folder
    select * from {{ ref('dim_stops_snapshot') }}
),

final as (
    select
        -- Use the dbt-generated surrogate key from the snapshot
        {{ dbt_utils.generate_surrogate_key(['stop_id', 'dbt_valid_from']) }} as stop_key,
        stop_id,
        stop_name,
        latitude,
        longitude,
        zone_id,
        stop_code,
        
        -- Mapping dbt internal snapshot columns to business names
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, cast('{{ var("scd_end_date") }}' as date)) as valid_to,
        (dbt_valid_to is null) as is_current
    from snapshot_source
)

select * from final