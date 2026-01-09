/*
    Intermediate model: Vehicle journeys sessionization.
    
    Groups telemetry events into logical journeys per vehicle per day.
    Computes journey-level aggregates for analysis.
*/

with enriched as (
    select * from {{ ref('stg_enriched') }}
),

journeys as (
    select
        -- Journey identification
        vehicle_id,
        event_date as journey_date,
        vehicle_id || '-' || cast(event_date as varchar) as journey_id,
        line_id,
        
        -- Temporal bounds
        min(event_timestamp) as journey_start,
        max(event_timestamp) as journey_end,
        
        -- Event counts
        count(*) as event_count,
        
        -- Delay metrics
        avg(delay_seconds) as avg_delay_seconds,
        max(delay_seconds) as max_delay_seconds,
        min(delay_seconds) as min_delay_seconds,
        stddev(delay_seconds) as stddev_delay_seconds,
        
        -- On-time performance
        sum(case when delay_category = 'on_time' then 1 else 0 end) as on_time_count,
        sum(case when delay_category = 'delayed' then 1 else 0 end) as delayed_count,
        sum(case when delay_category = 'early' then 1 else 0 end) as early_count,
        
        -- Speed metrics
        avg(speed_kmh) as avg_speed_kmh,
        max(speed_kmh) as max_speed_kmh,
        
        -- Distance
        sum(coalesce(distance_since_last_m, 0)) as total_distance_m,
        
        -- Stops
        count(distinct next_stop_id) as unique_stops_visited
        
    from enriched
    group by 
        vehicle_id,
        event_date,
        line_id
)

select
    *,
    -- Derived metrics
    total_distance_m / 1000.0 as total_distance_km,
    on_time_count * 100.0 / nullif(event_count, 0) as on_time_percentage,
    extract(epoch from (journey_end - journey_start)) / 3600.0 as journey_duration_hours
from journeys