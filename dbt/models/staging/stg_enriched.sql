/*
    Staging model for enriched events.
    Pattern: Quality as Firewall
*/

with source as (
    select * from {{ source('bronze', 'enriched') }}
),

cleaned as (
    select
        -- Primary identifiers
        vehicle_id,
        event_time_ms,
        
        -- Parsed timestamps
        to_timestamp(event_time_ms / 1000) as event_timestamp,
        cast(date_trunc('day', to_timestamp(event_time_ms / 1000)) as date) as event_date,
        
        -- Location
        latitude,
        longitude,
        
        -- Speed conversions
        coalesce(speed_ms, 0) as speed_ms,
        coalesce(speed_ms, 0) * 3.6 as speed_kmh,
        
        -- Raw Delay (Categorization moved to Intermediate/Marts)
        coalesce(delay_seconds, 0) as delay_seconds,
        
        -- Attributes
        coalesce(heading, 0) as heading,
        coalesce(door_status, 0) as door_status,
        line_id,
        direction_id,
        operator_id,
        next_stop_id,
        
        -- Flink Metrics
        delay_trend,
        speed_trend,
        distance_since_last_ms,
        time_since_last_ms,
        is_stopped,
        stopped_duration_ms,
        
        -- Metadata
        processing_time,
        kafka_partition,
        kafka_offset,
        ingestion_time,
        date as partition_date
        
    from source
    where 
        vehicle_id is not null
        and event_time_ms is not null
        and latitude is not null
        and longitude is not null
        -- Boundary filtering
        and latitude between 59.9 and 60.4
        and longitude between 24.5 and 25.2
        and (delay_seconds is null 
             or delay_seconds between {{ var('min_delay_seconds') }} and {{ var('max_delay_seconds') }})
)

select * from cleaned