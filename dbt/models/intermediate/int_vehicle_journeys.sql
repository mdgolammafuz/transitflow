/*
    Intermediate: Vehicle journey sessionization.
    Harden: Consistent type casting for contract enforcement and total_distance calculation.
*/

with enriched as (
    select * from {{ ref('stg_enriched') }}
),

journeys as (
    select
        vehicle_id,
        event_date as journey_date,
        -- Unique identifier for the journey session
        {{ dbt_utils.generate_surrogate_key(['vehicle_id', 'event_date', 'line_id']) }} as journey_id,
        line_id,
        
        min(event_timestamp) as journey_start,
        max(event_timestamp) as journey_end,
        count(*) as event_count,
        
        -- Aggregates
        avg(delay_seconds) as avg_delay_seconds,
        
        -- Performance categorization using macro
        sum(case when {{ classify_delay('delay_seconds') }} = 'on_time' then 1 else 0 end) as on_time_count,
        sum(case when {{ classify_delay('delay_seconds') }} = 'delayed' then 1 else 0 end) as delayed_count,
        sum(case when {{ classify_delay('delay_seconds') }} = 'early' then 1 else 0 end) as early_count,
        
        avg(speed_kmh) as avg_speed_kmh,
        sum(coalesce(distance_since_last_ms, 0)) as total_distance_ms
        
    from enriched
    group by vehicle_id, event_date, line_id
),

final as (
    select
        *,
        -- Conversion to KM (Double Precision)
        cast(total_distance_ms / 1000000.0 as double precision) as total_distance_km,
        
        -- Percentage logic with division safety
        cast(
            on_time_count * 100.0 / nullif(event_count, 0) 
        as double precision) as on_time_percentage,
        
        -- Duration calculation in decimal hours
        cast(
            extract(epoch from (journey_end - journey_start)) / 3600.0 
        as double precision) as journey_duration_hours
    from journeys
)

select * from final