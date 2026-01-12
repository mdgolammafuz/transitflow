/*
    Intermediate: Stop performance features for ML.
*/

with stop_events as (
    select * from {{ ref('stg_stop_events') }}
),

stop_metrics as (
    select
        stop_id,
        line_id,
        extract(hour from arrival_timestamp) as hour_of_day,
        extract(dow from arrival_timestamp) as day_of_week,
        
        count(*) as arrival_count,
        count(distinct vehicle_id) as unique_vehicles,
        
        -- Statistics
        avg(delay_at_arrival) as avg_delay_seconds,
        stddev(delay_at_arrival) as stddev_delay_seconds,
        
        -- Delay Distribution using centralized Macro
        sum(case when {{ classify_delay('delay_at_arrival') }} = 'on_time' then 1 else 0 end) as on_time_count,
        sum(case when {{ classify_delay('delay_at_arrival') }} = 'delayed' then 1 else 0 end) as delayed_count,
        
        avg(dwell_time_ms) as avg_dwell_time_ms,
        sum(case when door_opened then 1 else 0 end) as door_opened_count
        
    from stop_events
    group by 1, 2, 3, 4
)

select
    *,
    on_time_count * 100.0 / nullif(arrival_count, 0) as on_time_percentage
from stop_metrics