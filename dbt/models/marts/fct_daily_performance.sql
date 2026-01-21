/*
    Fact Table: Daily Performance
    Hardening: Aligned with _marts.yml contract.
    Source: raw_lakehouse.fct_daily_performance (Spark Gold Sink)
*/

{{ config(
    materialized='table',
    schema='marts'
) }}

with raw_daily as (
    select * from {{ source('raw_lakehouse', 'fct_daily_performance') }}
),

final as (
    select
        -- Aligned with YAML contract name: metric_id
        {{ dbt_utils.generate_surrogate_key(['line_id', 'date']) }} as metric_id,
        
        cast(line_id as text) as line_id,
        -- Aligned with YAML contract name: metric_date
        cast(date as date) as metric_date,
        
        -- Volumetric Metrics
        cast(total_events as bigint) as total_events,
        cast(0 as bigint) as total_journeys, 
        cast(0 as bigint) as unique_vehicles, 
        
        -- Performance Metrics
        cast(avg_delay_seconds as double precision) as avg_delay_seconds,
        cast(on_time_percentage as double precision) as on_time_percentage,
        
        -- Spatial/Distance
        cast(0.0 as double precision) as total_distance_km
        
    from raw_daily
    where line_id is not null
      and date is not null
)

select * from final