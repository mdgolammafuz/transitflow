/*
    Staging: Stop Events (Aggregated)
    Hardening: Strictly aligned with verified psql schema of public.fct_stop_arrivals.
    Note: 'date' column removed as it is physically missing from the Spark Sink.
*/

with source as (
    select * from {{ source('raw_lakehouse', 'fct_stop_arrivals') }}
),

cleaned as (
    select
        -- Identifiers (No aliasing)
        cast(stop_id as text) as stop_id,
        cast(line_id as text) as line_id,
        
        -- Time Context: 'date' removed because it is missing from the physical table.
        cast(hour_of_day as integer) as hour_of_day,
        cast(day_of_week as integer) as day_of_week,
        
        -- Aggregated metrics: Using Spark-native names directly.
        cast(historical_arrival_count as bigint) as historical_arrival_count,
        cast(historical_avg_delay as double precision) as historical_avg_delay,
        cast(avg_dwell_time_ms as double precision) as avg_dwell_time_ms,

        -- Spatial: Aligned with physical Spark Sink names but aliased for Warehouse consistency
        cast(latitude as double precision) as stop_lat,
        cast(longitude as double precision) as stop_lon,
        
        -- Metadata: Naive Timestamp (UTC Lock via profiles.yml)
        cast(current_timestamp as timestamp) as staging_time
        
    from source
    where stop_id is not null
      and line_id is not null
)

select * from cleaned