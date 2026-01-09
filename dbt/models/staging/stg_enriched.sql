/*
    Staging model for enriched events.
    
    Pattern: Quality as Firewall
    - Clean and type-cast source data
    - Add derived columns
    - Filter out invalid records
*/

with source as (
    select * from {{ source('bronze', 'enriched') }}
),

cleaned as (
    select
        -- Primary identifiers
        vehicle_id,
        event_time_ms,
        
        -- Parsed timestamp
        to_timestamp(event_time_ms / 1000) as event_timestamp,
        cast(date_trunc('day', to_timestamp(event_time_ms / 1000)) as date) as event_date,
        
        -- Location (validated in tests)
        latitude,
        longitude,
        
        -- Speed conversions
        coalesce(speed_ms, 0) as speed_ms,
        coalesce(speed_ms, 0) * 3.6 as speed_kmh,
        
        -- Delay
        coalesce(delay_seconds, 0) as delay_seconds,
        
        -- Delay categorization
        case
            when abs(coalesce(delay_seconds, 0)) <= {{ var('on_time_threshold') }} then 'on_time'
            when coalesce(delay_seconds, 0) > {{ var('delayed_threshold') }} then 'delayed'
            when coalesce(delay_seconds, 0) < -{{ var('delayed_threshold') }} then 'early'
            else 'slight_delay'
        end as delay_category,
        
        -- Boolean: is significantly delayed?
        coalesce(delay_seconds, 0) > {{ var('delayed_threshold') }} as is_delayed,
        
        -- Other fields
        coalesce(heading, 0) as heading,
        coalesce(door_status, 0) as door_status,
        line_id,
        direction_id,
        operator_id,
        next_stop_id,
        
        -- Computed features from Flink
        delay_trend,
        speed_trend,
        distance_since_last_m,
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
        -- Filter out records with null required fields
        vehicle_id is not null
        and event_time_ms is not null
        and latitude is not null
        and longitude is not null
        -- Filter out records outside Helsinki bbox
        and latitude between 59.9 and 60.4
        and longitude between 24.5 and 25.2
        -- Filter out unrealistic delays
        and (delay_seconds is null 
             or delay_seconds between {{ var('min_delay_seconds') }} and {{ var('max_delay_seconds') }})
)

select * from cleaned