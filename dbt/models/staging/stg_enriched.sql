/*
    Staging model for enriched events.
    Audited: Including 'date' column for partition-aware downstream models.
    Principal Fix: Added trim to next_stop_id to ensure join reliability during discovery.
*/

with source as (
    select * from {{ source('bronze', 'enriched') }}
),

cleaned as (
    select
        -- Identifiers
        cast(vehicle_id as text) as vehicle_id,
        cast(event_time_ms as bigint) as event_time_ms,
        
        -- DERIVED: event_timestamp from ms
        to_timestamp(event_time_ms / 1000.0)::timestamp as event_timestamp,
        
        -- Location
        cast(latitude as double precision) as latitude,
        cast(longitude as double precision) as longitude,
        
        -- Physical Attributes
        cast(coalesce(speed_ms, 0) as double precision) as speed_ms,
        cast((coalesce(speed_ms, 0) * 3.6) as double precision) as speed_kmh,
        cast(coalesce(delay_seconds, 0) as integer) as delay_seconds,
        
        -- Attributes
        cast(coalesce(heading, 0) as double precision) as heading,
        cast(coalesce(door_status::int, 0) as integer) as door_status,
        cast(line_id as text) as line_id,
        cast(direction_id as text) as direction_id,
        cast(operator_id as text) as operator_id,
        
        -- FIXED: Trimmed for join reliability in Discovery Dimension
        trim(cast(next_stop_id as text)) as next_stop_id,
        
        -- Metrics
        cast(delay_trend as text) as delay_trend,
        cast(speed_trend as text) as speed_trend,
        cast(distance_since_last_m as double precision) as distance_since_last_ms,
        cast(time_since_last_ms as bigint) as time_since_last_ms,
        cast(coalesce(is_stopped, false) as boolean) as is_stopped,
        cast(coalesce(stopped_duration_ms, 0) as bigint) as stopped_duration_ms,
        
        -- Metadata
        to_timestamp(processing_time / 1000.0)::timestamp as processing_time,
        cast(kafka_partition as integer) as kafka_partition,
        cast(kafka_offset as bigint) as kafka_offset,
        cast(ingestion_time as timestamp) as ingestion_time,

        -- REQUIRED: Pass through the partition date from Bronze
        cast(date as date) as date
        
    from source
    where 
        vehicle_id is not null
        and event_time_ms is not null
        and latitude is not null
        and longitude is not null
        -- Helsinki Geo-fence
        and latitude between 59.9 and 60.4
        and longitude between 24.5 and 25.2
)

select * from cleaned