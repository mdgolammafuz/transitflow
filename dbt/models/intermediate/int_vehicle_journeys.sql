/*
    Intermediate: Vehicle journey sessionization.
    Hardening: Derived temporal context and category logic (Temporal Anchor).
    Time Fix: UTC-locked epoch calculations from event_time_ms.
*/

with enriched as (
    select * from {{ ref('stg_enriched') }}
),

journeys as (
    select
        cast(vehicle_id as text) as vehicle_id,
        -- Temporal Anchor: Derived from ingestion_time
        cast(ingestion_time as date) as journey_date,
        -- Robust Journey ID: Deterministic surrogate key for OCI/Cron auditability
        {{ dbt_utils.generate_surrogate_key(['vehicle_id', 'ingestion_time', 'line_id', 'operator_id']) }} as journey_id,
        cast(line_id as text) as line_id,
        cast(operator_id as text) as operator_id,
        
        -- Time Fix: Deriving timestamps from Unix epoch (event_time_ms)
        min(to_timestamp(event_time_ms / 1000.0)) as journey_start,
        max(to_timestamp(event_time_ms / 1000.0)) as journey_end,
        count(*) as event_count,
        
        -- Aggregates (No renaming)
        avg(delay_seconds) as avg_delay_seconds,
        
        -- Re-calculating categorizations since they are missing from raw source
        sum(case 
            when delay_seconds <= {{ var('on_time_threshold_seconds') }} then 1 
            else 0 
        end) as on_time_count,
        sum(case 
            when delay_seconds > {{ var('on_time_threshold_seconds') }} 
             and delay_seconds <= {{ var('late_threshold_seconds') }} then 1 
            else 0 
        end) as delayed_count,
        sum(case 
            when delay_seconds > {{ var('late_threshold_seconds') }} then 1 
            else 0 
        end) as early_count, -- Logic follows your provided sums
        
        avg(speed_kmh) as avg_speed_kmh,
        sum(coalesce(distance_since_last_ms, 0)) as total_distance_units
        
    from enriched
    group by vehicle_id, ingestion_time, line_id, operator_id
),

final as (
    select
        *,
        -- Metric: Distance in KM (Assuming Meters from Spark source)
        cast(total_distance_units / 1000.0 as double precision) as total_distance_km,
        
        -- Percentage logic: Enforced Double Precision for Mart compatibility
        cast(
            on_time_count * 100.0 / nullif(event_count, 0) 
        as double precision) as on_time_percentage,
        
        -- Duration: Pure UTC-safe epoch extraction
        cast(
            extract(epoch from (journey_end - journey_start)) / 3600.0 
        as double precision) as journey_duration_hours
    from journeys
)

select * from final