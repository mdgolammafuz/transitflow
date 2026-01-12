with source as (
    select * from {{ source('bronze', 'stop_events') }}
),

cleaned as (
    select
        vehicle_id,
        stop_id,
        stop_code,
        stop_name,
        zone_id,
        operator_id,
        line_id,
        direction_id,
        arrival_time,
        to_timestamp(arrival_time / 1000) as arrival_timestamp,
        cast(date_trunc('day', to_timestamp(arrival_time / 1000)) as date) as arrival_date,
        delay_at_arrival,
        dwell_time_ms,
        distance_since_last_ms,
        stop_sequence,
        door_opened,
        latitude,
        longitude,
        kafka_partition,
        kafka_offset,
        ingestion_time,
        date as partition_date
    from source
    where vehicle_id is not null
      and stop_id is not null
      and arrival_time is not null
      and delay_at_arrival between {{ var('min_delay_seconds') }} and {{ var('max_delay_seconds') }}
)

select * from cleaned