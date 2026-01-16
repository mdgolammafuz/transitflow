/*
    Intermediate Model: int_stop_performance
    Context: Bridges Staging and Marts for ML Feature Engineering.
    Methodical: Updated to pass through Ingredient B (GPS) and unified historical naming.
*/

{{ config(
    materialized='view',
    schema='intermediate'
) }}

with stop_metrics as (
    -- This now contains historical_avg_delay, latitude, and longitude
    select * from {{ ref('stg_stop_events') }}
),

final as (
    select
        stop_id,
        line_id,
        cast(hour_of_day as integer) as hour_of_day,
        cast(day_of_week as integer) as day_of_week,
        
        -- Grain: sample_count for Feature Store naming
        cast(sample_count as bigint) as sample_count,
        
        -- Features: Directly mapped from Staging (no re-renaming needed now)
        cast(historical_avg_delay as double precision) as historical_avg_delay,
        
        -- Standard Deviation: Placeholder for Phase 6 enhancements
        cast(0.0 as double precision) as historical_stddev_delay,
        
        -- Units: Keep ms for dwell time as per Spark output
        cast(avg_dwell_time_ms as double precision) as avg_dwell_time_ms,
        
        -- On-time percentage: Placeholder until Spark logic is expanded
        cast(100.0 as double precision) as on_time_percentage,

        -- Ingredient B: Spatial Data (MUST be passed through to the Mart)
        cast(latitude as double precision) as latitude,
        cast(longitude as double precision) as longitude
        
    from stop_metrics
)

select * from final