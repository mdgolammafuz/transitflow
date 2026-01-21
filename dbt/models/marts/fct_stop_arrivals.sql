/*
    Mart: fct_stop_arrivals
    Context: Final Feature Store table for ML Serving.
    Hardening: Grain aligned to individual Kafka events to preserve all 15,903 arrivals.
    Fix: Surrogate key uses Kafka offset/partition for 100% uniqueness.
    Note: Metadata columns (kafka_partition, kafka_offset) included in final SELECT to satisfy the model contract.
*/

{{ config(
    materialized='table', 
    schema='marts',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (feature_id)"
    ]
) }}

with stop_performance as (
    -- Pulling from Intermediate (Grain: individual stop event)
    select 
        stop_id,
        line_id,
        date, 
        hour_of_day,
        day_of_week,
        delay_category,
        historical_arrival_count,
        historical_avg_delay,
        stddev_delay,
        on_time_percentage,
        avg_dwell_time_ms,
        stop_lat,
        stop_lon,
        processing_time,
        ingestion_time,
        kafka_partition,
        kafka_offset
    from {{ ref('int_stop_performance') }}
),

stops as (
    -- Dynamic Discovery Dimension
    select 
        trim(cast(stop_id as text)) as stop_id,
        stop_name,
        zone_id,
        stop_code
    from {{ ref('dim_stops') }} 
),

lines as (
    -- Static Line Dimension
    select 
        trim(cast(line_id as text)) as line_id,
        line_name,
        line_type
    from {{ ref('dim_lines') }} 
    where is_current = true
),

final as (
    select
        -- PRIMARY KEY: Guaranteed unique via Kafka markers + Business Keys
        {{ dbt_utils.generate_surrogate_key([
            'sp.kafka_partition',
            'sp.kafka_offset',
            'sp.stop_id',
            'sp.line_id'
        ]) }} as feature_id,
        
        sp.stop_id,
        sp.line_id,
        sp.date as event_date, 
        sp.hour_of_day,
        sp.day_of_week,
        sp.delay_category,
        
        -- Metrics: Casted for downstream ML Schema Enforcement
        cast(sp.historical_arrival_count as bigint) as historical_arrival_count,
        cast(sp.historical_avg_delay as double precision) as historical_avg_delay,
        cast(sp.stddev_delay as double precision) as historical_stddev_delay,
        cast(sp.on_time_percentage as double precision) as historical_on_time_pct,
        cast(sp.avg_dwell_time_ms as double precision) as avg_dwell_time_ms,
        
        cast(sp.stop_lat as double precision) as stop_lat,
        cast(sp.stop_lon as double precision) as stop_lon,
        
        -- Dimension Metadata
        s.stop_name,
        s.zone_id,
        s.stop_code,
        
        coalesce(l.line_name, 'Line: ' || sp.line_id) as line_name,
        coalesce(l.line_type, 'Unknown') as line_type,

        -- Audit Metadata (Required by Enforced Contract)
        sp.processing_time,
        sp.ingestion_time,

        -- Kafka Metadata (Required by Enforced Contract for Uniqueness Test)
        sp.kafka_partition,
        sp.kafka_offset
        
    from stop_performance sp
    -- INNER JOIN is safe because dim_stops uses Dynamic Discovery
    inner join stops s on sp.stop_id = s.stop_id
    -- LEFT JOIN on lines as discovery is not yet implemented for lines
    left join lines l on sp.line_id = l.line_id
)

select * from final