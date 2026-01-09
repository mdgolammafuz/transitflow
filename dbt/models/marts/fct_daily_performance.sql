/*
    Fact Table: Daily Performance
    
    Daily aggregated metrics by line for dashboards and reporting.
*/

with journeys as (
    select * from {{ ref('int_vehicle_journeys') }}
),

daily_metrics as (
    select
        line_id,
        journey_date as metric_date,
        
        -- Volume metrics
        count(*) as total_journeys,
        sum(event_count) as total_events,
        count(distinct vehicle_id) as unique_vehicles,
        
        -- Delay metrics
        avg(avg_delay_seconds) as avg_delay_seconds,
        max(max_delay_seconds) as max_delay_seconds,
        min(min_delay_seconds) as min_delay_seconds,
        
        -- Performance metrics
        avg(on_time_percentage) as on_time_percentage,
        sum(on_time_count) as total_on_time,
        sum(delayed_count) as total_delayed,
        sum(early_count) as total_early,
        
        -- Speed metrics
        avg(avg_speed_kmh) as avg_speed_kmh,
        max(max_speed_kmh) as max_speed_kmh,
        
        -- Distance metrics
        sum(total_distance_km) as total_distance_km
        
    from journeys
    where line_id is not null
    group by line_id, journey_date
)

select
    {{ dbt_utils.generate_surrogate_key(['line_id', 'metric_date']) }} as metric_id,
    *
from daily_metrics