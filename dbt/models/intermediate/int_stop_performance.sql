/*
    Intermediate Model: int_stop_performance
    Principal Alignment: Adjusted grain to Arrival level (Preserving the 15,903 events).
    Hardening: Restoring the Signal by replacing placeholders with actual window functions.
    Logic: Derives Standard Deviation and On-Time % directly from telemetry to fix the Zero Signal.
*/

{{ config(
    materialized='view',
    schema='intermediate'
) }}

with stopped_events as (
    -- The source of truth: 15,903 verified stop events
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
    -- Contextual data (Source: Spark Sink)
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
            when s.delay_seconds <= {{ var('on_time_threshold_seconds', 60) }} then 'ON_TIME'
            when s.delay_seconds <= {{ var('late_threshold_seconds', 300) }} then 'LATE'
            else 'VERY_LATE'
        end as delay_category,
        
        -- RESTORED SIGNAL: Standard Deviation calculation (Population)
        stddev_pop(s.delay_seconds) over (
            partition by s.stop_id, s.line_id, s.hour_of_day
        )::double precision as stddev_delay,

        -- RESTORED SIGNAL: On-time percentage calculation
        (count(*) filter (where s.delay_seconds <= {{ var('on_time_threshold_seconds', 60) }}) over (
            partition by s.stop_id, s.line_id, s.hour_of_day
        ) * 100.0 / nullif(count(*) over (
            partition by s.stop_id, s.line_id, s.hour_of_day
        ), 0))::double precision as on_time_percentage,

        -- Dwell time: Default to 0.0 if not found in metrics, 
        -- but preserved as a field for ML feature consistency
        coalesce(m.avg_dwell_time_ms, 0.0)::double precision as avg_dwell_time_ms,

        -- Spatial: Use metrics value if available, fallback to telemetry coordinates
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