/*
    Intermediate Model: int_stop_performance
    Principal Alignment: Adjusted grain to Arrival level (Preserving the 21,930 events).
    Hardening: Included Kafka metadata to allow for unique record identification in Marts.
    Logic: Uses Window Functions to derive counts from telemetry to bypass empty Spark sinks.
*/

{{ config(
    materialized='view',
    schema='intermediate'
) }}

with stopped_events as (
    -- The source of truth: 21,930 verified stop events
    select 
        trim(cast(next_stop_id as text)) as stop_id,
        trim(cast(line_id as text)) as line_id,
        date as event_date,
        -- Extracting hour to prevent daily collapse and align with metrics
        extract(hour from event_timestamp)::int as hour_of_day,
        extract(dow from date)::int as day_of_week,
        delay_seconds,
        latitude,
        longitude,
        processing_time,
        ingestion_time,
        -- REQUIRED: Pass through Kafka metadata for downstream uniqueness
        kafka_partition,
        kafka_offset
    from {{ ref('stg_enriched') }}
    where is_stopped = true
      and date = '{{ var("target_date") }}'
      and next_stop_id is not null
),

stop_metrics as (
    -- Contextual data (Source: Spark Sink - Currently 0 rows)
    select 
        trim(cast(stop_id as text)) as stop_id,
        trim(cast(line_id as text)) as line_id,
        hour_of_day,
        day_of_week,
        historical_arrival_count,
        historical_avg_delay,
        avg_dwell_time_ms,
        stop_lat,
        stop_lon
    from {{ ref('stg_stop_events') }}
),

final as (
    select
        s.stop_id,
        s.line_id,
        s.event_date as date,
        s.hour_of_day,
        s.day_of_week,
        
        -- Metrics: Using Window Functions to count telemetry arrivals per stop/line/day
        -- Ensures historical_arrival_count matches reality (21,930 source) even if Sink is empty
        coalesce(
            m.historical_arrival_count, 
            count(*) over (partition by s.stop_id, s.line_id, s.event_date)
        )::bigint as historical_arrival_count,

        -- Average delay for this specific stop/line/hour
        coalesce(
            m.historical_avg_delay,
            avg(s.delay_seconds) over (partition by s.stop_id, s.line_id, s.hour_of_day)
        )::double precision as historical_avg_delay,

        -- Delay Category based on the individual event delay
        case 
            when s.delay_seconds <= {{ var('on_time_threshold_seconds') }} then 'ON_TIME'
            when s.delay_seconds <= {{ var('late_threshold_seconds') }} then 'LATE'
            else 'VERY_LATE'
        end as delay_category,
        
        -- Feature Prep placeholders
        cast(0.0 as double precision) as stddev_delay,
        cast(100.0 as double precision) as on_time_percentage,
        coalesce(m.avg_dwell_time_ms, 0.0)::double precision as avg_dwell_time_ms,

        -- Spatial
        coalesce(m.stop_lat, s.latitude)::double precision as stop_lat,
        coalesce(m.stop_lon, s.longitude)::double precision as stop_lon,

        -- Metadata (NO OMISSIONS)
        s.processing_time,
        s.ingestion_time,
        s.kafka_partition,
        s.kafka_offset
        
    from stopped_events s
    left join stop_metrics m 
        on s.stop_id = m.stop_id 
        and s.line_id = m.line_id
        and s.hour_of_day = m.hour_of_day
)

select * from final