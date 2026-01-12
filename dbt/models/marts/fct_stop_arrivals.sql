/*
    Fact Table: Stop Arrivals
    Joined with dimensions and historical features.
*/

with stop_events as (
    select * from {{ ref('stg_stop_events') }}
),

stop_performance as (
    select * from {{ ref('int_stop_performance') }}
),

stops as (
    select * from {{ ref('dim_stops') }}
),

lines as (
    select * from {{ ref('dim_lines') }}
),

enriched_arrivals as (
    select
        {{ dbt_utils.generate_surrogate_key(['se.vehicle_id', 'se.stop_id', 'se.arrival_time']) }} as arrival_id,
        
        se.vehicle_id,
        se.stop_id,
        se.line_id,
        se.direction_id,
        se.arrival_time,
        se.arrival_timestamp,
        se.arrival_date,
        
        -- Target variable and categorization via macro
        se.delay_at_arrival,
        {{ classify_delay('se.delay_at_arrival') }} as delay_category,
        
        se.dwell_time_ms,
        se.door_opened,
        
        -- Dimension attributes (Explicitly sourced from Dimensions)
        s.stop_name,
        s.zone_id,
        l.line_name,
        l.line_type,
        
        -- Historical features
        sp.avg_delay_seconds as historical_avg_delay,
        sp.stddev_delay_seconds as historical_stddev_delay,
        sp.on_time_percentage as historical_on_time_pct,
        sp.arrival_count as historical_arrival_count
        
    from stop_events se
    left join stops s 
        on se.stop_id = s.stop_id 
        and se.arrival_timestamp >= s.valid_from 
        and se.arrival_timestamp < s.valid_to
    left join lines l 
        on se.line_id = l.line_id
        and se.arrival_timestamp >= l.valid_from
        and se.arrival_timestamp < l.valid_to
    left join stop_performance sp 
        on se.stop_id = sp.stop_id
        and se.line_id = sp.line_id
        and extract(hour from se.arrival_timestamp) = sp.hour_of_day
        and extract(dow from se.arrival_timestamp) = sp.day_of_week
)

select * from enriched_arrivals