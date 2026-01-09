/*
    Intermediate model: Stop-level performance metrics.
    
    Pattern: ML Reproducibility
    These metrics become historical features for ML training.
    
    Aggregated by:
    - stop_id
    - line_id  
    - hour_of_day (0-23)
    - day_of_week (0-6, Monday=0)
*/

with stop_events as (
    select * from {{ ref('stg_stop_events') }}
),

stop_metrics as (
    select
        -- Grouping keys
        stop_id,
        line_id,
        extract(hour from arrival_timestamp) as hour_of_day,
        extract(dow from arrival_timestamp) as day_of_week,
        
        -- Arrival counts
        count(*) as arrival_count,
        count(distinct vehicle_id) as unique_vehicles,
        
        -- Delay metrics (for ML features)
        avg(delay_at_arrival) as avg_delay_seconds,
        stddev(delay_at_arrival) as stddev_delay_seconds,
        min(delay_at_arrival) as min_delay_seconds,
        max(delay_at_arrival) as max_delay_seconds,
        percentile_cont(0.5) within group (order by delay_at_arrival) as median_delay_seconds,
        percentile_cont(0.9) within group (order by delay_at_arrival) as p90_delay_seconds,
        
        -- On-time performance
        sum(case when delay_category = 'on_time' then 1 else 0 end) as on_time_count,
        sum(case when delay_category = 'delayed' then 1 else 0 end) as delayed_count,
        
        -- Dwell time
        avg(dwell_time_ms) as avg_dwell_time_ms,
        
        -- Door activity
        sum(case when door_opened then 1 else 0 end) as door_opened_count
        
    from stop_events
    where stop_id is not null and line_id is not null
    group by 
        stop_id,
        line_id,
        extract(hour from arrival_timestamp),
        extract(dow from arrival_timestamp)
)

select
    *,
    -- Derived metrics
    on_time_count * 100.0 / nullif(arrival_count, 0) as on_time_percentage,
    door_opened_count * 100.0 / nullif(arrival_count, 0) as door_open_percentage
from stop_metrics