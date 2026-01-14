/*
    Staging model for enriched events.
    Pattern: Quality as Firewall (Enforced Contract)
    Source: bronze.enriched (S3/MinIO via Spark)
*/

with source as (
    select * from {{ source('bronze', 'enriched') }}
),

cleaned as (
    select
        -- Primary identifiers
        cast(vehicle_id as text) as vehicle_id,
        cast(event_time_ms as bigint) as event_time_ms,
        
        -- Parsed timestamps (Postgres-specific epoch conversion)
        cast(to_timestamp(event_time_ms / 1000.0) as timestamp with time zone) as event_timestamp,
        cast(date_trunc('day', to_timestamp(event_time_ms / 1000.0)) as date) as event_date,
        
        -- Location
        cast(latitude as double precision) as latitude,
        cast(longitude as double precision) as longitude,
        
        -- Speed conversions
        cast(coalesce(speed_ms, 0) as double precision) as speed_ms,
        cast(coalesce(speed_ms, 0) * 3.6 as double precision) as speed_kmh,
        
        -- Raw Delay
        cast(coalesce(delay_seconds, 0) as integer) as delay_seconds,
        
        -- Attributes
        cast(coalesce(heading, 0) as double precision) as heading,
        cast(coalesce(door_status, 0) as integer) as door_status,
        cast(line_id as text) as line_id,
        cast(direction_id as text) as direction_id,
        cast(operator_id as text) as operator_id,
        cast(next_stop_id as text) as next_stop_id,
        
        -- Flink Metrics
        cast(delay_trend as text) as delay_trend,
        cast(speed_trend as text) as speed_trend,
        cast(distance_since_last_ms as double precision) as distance_since_last_ms,
        cast(time_since_last_ms as bigint) as time_since_last_ms,
        cast(coalesce(is_stopped, false) as boolean) as is_stopped,
        cast(coalesce(stopped_duration_ms, 0) as bigint) as stopped_duration_ms,
        
        -- Metadata
        cast(processing_time as timestamp with time zone) as processing_time,
        cast(kafka_partition as integer) as kafka_partition,
        cast(kafka_offset as bigint) as kafka_offset,
        cast(ingestion_time as timestamp with time zone) as ingestion_time,
        cast(date as date) as partition_date
        
    from source
    where 
        vehicle_id is not null
        and event_time_ms is not null
        and latitude is not null
        and longitude is not null
        -- Boundary filtering for Helsinki region
        and latitude between 59.9 and 60.4
        and longitude between 24.5 and 25.2
        and (delay_seconds is null 
             or delay_seconds between {{ var('min_delay_seconds') }} and {{ var('max_delay_seconds') }})
)

select * from cleaned