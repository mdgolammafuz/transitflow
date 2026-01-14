{{ config(
    materialized='table', 
    schema='marts',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (feature_id)"
    ]
) }}

with stop_performance as (
    -- Clean the incoming IDs from the Spark/Intermediate layer
    select 
        trim(cast(stop_id as text)) as stop_id,
        trim(cast(line_id as text)) as line_id,
        hour_of_day,
        day_of_week,
        sample_count,
        historical_avg_delay,
        historical_stddev_delay,
        on_time_percentage,
        avg_dwell_time_ms
    from {{ ref('int_stop_performance') }}
),

stops as (
    -- Clean the IDs from the dimension table
    select 
        trim(cast(stop_id as text)) as stop_id,
        stop_name,
        zone_id,
        latitude,
        longitude,
        stop_code
    from {{ ref('dim_stops') }} 
    where is_current = true
),

lines as (
    select 
        trim(cast(line_id as text)) as line_id,
        line_name,
        line_type
    from {{ ref('dim_lines') }} 
    where is_current = true
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'sp.stop_id', 
            'sp.line_id', 
            'sp.hour_of_day', 
            'sp.day_of_week'
        ]) }} as feature_id,
        
        sp.stop_id,
        sp.line_id,
        sp.hour_of_day,
        sp.day_of_week,
        
        cast(sp.sample_count as bigint) as historical_arrival_count,
        cast(sp.historical_avg_delay as double precision) as historical_avg_delay,
        cast(sp.historical_stddev_delay as double precision) as historical_stddev_delay,
        cast(sp.on_time_percentage as double precision) as historical_on_time_pct,
        cast(sp.avg_dwell_time_ms as double precision) as avg_dwell_time_ms,
        
        s.stop_name,
        s.zone_id,
        cast(s.latitude as double precision) as latitude,
        cast(s.longitude as double precision) as longitude,
        s.stop_code,
        l.line_name,
        l.line_type
        
    from stop_performance sp
    inner join stops s on sp.stop_id = s.stop_id
    inner join lines l on sp.line_id = l.line_id
)

select * from final