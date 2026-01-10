/*
    Intermediate: Vehicle journey sessionization.
*/

with enriched as (
    select * from {{ ref('stg_enriched') }}
),

journeys as (
    select
        vehicle_id,
        event_date as journey_date,
        -- Secure identifier generation
        {{ dbt_utils.generate_surrogate_key(['vehicle_id', 'event_date']) }} as journey_id,
        line_id,
        
        min(event_timestamp) as journey_start,
        max(event_timestamp) as journey_end,
        count(*) as event_count,
        
        -- Aggregates
        avg(delay_seconds) as avg_delay_seconds,
        
        -- Performance categorization using macro
        sum(case when {{ classify_delay('delay_seconds') }} = 'on_time' then 1 else 0 end) as on_time_count,
        sum(case when {{ classify_delay('delay_seconds') }} = 'delayed' then 1 else 0 end) as delayed_count,
        
        avg(speed_kmh) as avg_speed_kmh,
        sum(coalesce(distance_since_last_m, 0)) as total_distance_m
        
    from enriched
    group by 1, 2, 4
)

select
    *,
    total_distance_m / 1000.0 as total_distance_km,
    extract(epoch from (journey_end - journey_start)) / 3600.0 as journey_duration_hours
from journeys