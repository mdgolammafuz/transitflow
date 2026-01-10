/*
    Fact Table: Daily Performance
*/

with journeys as (
    select * from {{ ref('int_vehicle_journeys') }}
),

daily_metrics as (
    select
        line_id,
        journey_date as metric_date,
        
        count(*) as total_journeys,
        sum(event_count) as total_events,
        count(distinct vehicle_id) as unique_vehicles,
        
        avg(avg_delay_seconds) as avg_delay_seconds,
        avg(on_time_percentage) as on_time_percentage,
        
        sum(total_distance_km) as total_distance_km
        
    from journeys
    where line_id is not null
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['line_id', 'metric_date']) }} as metric_id,
    *
from daily_metrics