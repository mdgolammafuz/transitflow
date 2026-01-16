/*
    Staging: Stop Events (Aggregated)
    This model aligns the Spark Gold JDBC sink with dbt's enforced contract.
    Source: public.fct_stop_arrivals (Spark Sink)
    Updated: Aligned with historical_* naming and Ingredient B coordinates.
*/

with source as (
    -- Reading from the Spark Sink in the public schema
    select * from {{ source('raw_lakehouse', 'fct_stop_arrivals') }}
),

cleaned as (
    select
        -- Casting stop_id to text to match the dimension and contract requirements
        cast(stop_id as text) as stop_id,
        cast(line_id as text) as line_id,
        
        -- Time-based dimensions from Spark groupBy
        cast(hour_of_day as integer) as hour_of_day,
        cast(day_of_week as integer) as day_of_week,
        
        -- Aggregated metrics (Renamed to match Spark Gold output)
        cast(historical_arrival_count as bigint) as sample_count,
        cast(historical_avg_delay as double precision) as historical_avg_delay,
        cast(avg_dwell_time_ms as double precision) as avg_dwell_time_ms,

        -- Ingredient B: Spatial Coordinates (Now coming from the Spark sink)
        cast(latitude as double precision) as latitude,
        cast(longitude as double precision) as longitude,
        
        -- Metadata
        cast(current_timestamp as timestamp with time zone) as ingestion_time
        
    from source
    where stop_id is not null
      and line_id is not null
)

select * from cleaned