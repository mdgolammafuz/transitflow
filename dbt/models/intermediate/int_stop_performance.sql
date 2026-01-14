/*
    Intermediate: Stop performance features for ML.
    Harden: Aligned with Spark Gold aggregated output and unified naming.
*/

{{ config(
    materialized='view',
    schema='intermediate'
) }}

with stop_metrics as (
    select * from {{ ref('stg_stop_events') }}
),

final as (
    select
        stop_id,
        line_id,
        cast(hour_of_day as integer) as hour_of_day,
        cast(day_of_week as integer) as day_of_week,
        
        -- Grain: sample_count matches the YAML test and Feature Store naming
        cast(sample_count as bigint) as sample_count,
        
        -- Features: Renamed to historical_ prefixed columns for the Mart
        cast(avg_delay as double precision) as historical_avg_delay,
        
        -- Default for stddev to satisfy downstream schema contracts
        cast(0.0 as double precision) as historical_stddev_delay,
        
        -- Units: Keep ms for dwell time as per Spark output
        cast(avg_dwell_time_ms as double precision) as avg_dwell_time_ms,
        
        -- On-time percentage: Placeholder until Spark provides classification counts
        cast(100.0 as double precision) as on_time_percentage
        
    from stop_metrics
)

select * from final