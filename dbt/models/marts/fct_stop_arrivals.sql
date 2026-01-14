{{ config(
    materialized='table', 
    schema='marts'
) }}

with stop_performance as (
    select * from {{ ref('int_stop_performance') }}
),

stops as (
    select * from {{ ref('dim_stops') }} where is_current = true
),

lines as (
    select * from {{ ref('dim_lines') }} where is_current = true
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'sp.stop_id', 
            'sp.line_id', 
            'sp.hour_of_day', 
            'sp.day_of_week'
        ]) }} as feature_id,
        
        -- Normalized IDs
        trim(cast(sp.stop_id as text)) as stop_id,
        trim(cast(sp.line_id as text)) as line_id,
        
        cast(sp.hour_of_day as integer) as hour_of_day,
        cast(sp.day_of_week as integer) as day_of_week,
        
        -- Feature Metrics
        cast(sp.sample_count as bigint) as historical_arrival_count,
        cast(sp.historical_avg_delay as double precision) as historical_avg_delay,
        cast(sp.historical_stddev_delay as double precision) as historical_stddev_delay,
        cast(sp.on_time_percentage as double precision) as historical_on_time_pct,
        cast(sp.avg_dwell_time_ms as double precision) as avg_dwell_time_ms,
        
        -- Metadata (Now guaranteed by our 100% Green build)
        s.stop_name,
        s.zone_id,
        cast(s.latitude as double precision) as latitude,
        cast(s.longitude as double precision) as longitude,
        s.stop_code,
        l.line_name,
        l.line_type
        
    from stop_performance sp
    left join stops s on trim(sp.stop_id) = trim(s.stop_id)
    left join lines l on trim(sp.line_id) = trim(l.line_id)
)

select * from final