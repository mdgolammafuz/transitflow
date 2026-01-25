{{ config(
    materialized='table',
    schema='marts'
) }}

with daily_agg as (
    select
        -- Granularity: One record per line per day
        line_id,
        journey_date as metric_date,
        
        -- Aggregations
        count(distinct journey_id) as total_journeys,
        sum(event_count) as total_events,
        count(distinct vehicle_id) as unique_vehicles,
        
        -- Average Delay
        avg(avg_delay_seconds) as avg_delay_seconds,
        
        -- Speed Calculation
        -- Sum(Total KM) / Sum(Total Hours) = Avg Speed (km/h)
        case 
            when sum(journey_duration_hours) > 0 
            then sum(total_distance_km) / sum(journey_duration_hours)
            else 0 
        end as avg_speed_kmh,

        -- On-Time Performance
        avg(on_time_percentage) as on_time_percentage,
        
        -- Total Distance
        sum(total_distance_km) as total_distance_km

    from {{ ref('int_vehicle_journeys') }}
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['line_id', 'metric_date']) }} as metric_id,
    line_id,
    metric_date,
    -- FIXED: Explicitly cast counts/sums to BigInt to match Contract
    cast(total_journeys as bigint) as total_journeys,
    cast(total_events as bigint) as total_events,
    cast(unique_vehicles as bigint) as unique_vehicles,
    -- Float Casts
    cast(avg_delay_seconds as double precision) as avg_delay_seconds,
    cast(avg_speed_kmh as double precision) as avg_speed_kmh,
    cast(on_time_percentage as double precision) as on_time_percentage,
    cast(total_distance_km as double precision) as total_distance_km
from daily_agg