/*
    Staging model for stop arrival events.
    Target variable for ML: delay_at_arrival.
*/

with source as (
    select * from {{ source('bronze', 'stop_events') }}
),

cleaned as (
    select
        vehicle_id,
        stop_id,
        
        -- Timestamps
        arrival_time,
        to_timestamp(arrival_time / 1000) as arrival_timestamp,
        cast(date_trunc('day', to_timestamp(arrival_time / 1000)) as date) as arrival_date,
        
        -- Raw ML Target
        delay_at_arrival,
        
        line_id,
        direction_id,
        dwell_time_ms,
        door_opened,
        latitude,
        longitude,
        
        -- Metadata
        kafka_partition,
        kafka_offset,
        ingestion_time,
        date as partition_date
        
    from source
    where 
        vehicle_id is not null
        and stop_id is not null
        and arrival_time is not null
        and delay_at_arrival is not null
        and delay_at_arrival between {{ var('min_delay_seconds') }} and {{ var('max_delay_seconds') }}
)

select * from cleaned