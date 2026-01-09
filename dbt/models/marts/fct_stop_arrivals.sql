/*
    Fact Table: Stop Arrivals
    
    Pattern: Semantic Interface
    
    Primary table for ML training:
    - Each row is a stop arrival event
    - delay_at_arrival is the target variable
    - Joined with dimensions for enrichment
    - Historical features from int_stop_performance
    
    Interview talking point:
    "This is where stream meets batch for ML. The stop arrival events 
    come from Flink CEP detecting when a vehicle arrives at a stop.
    We join with historical stop performance to create the feature vector.
    The delay_at_arrival becomes our training label."
*/

with stop_events as (
    select * from {{ ref('stg_stop_events') }}
),

stop_performance as (
    select * from {{ ref('int_stop_performance') }}
),

stops as (
    select * from {{ ref('dim_stops') }}
    where is_current = true
),

lines as (
    select * from {{ ref('dim_lines') }}
    where is_current = true
),

enriched as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['se.vehicle_id', 'se.stop_id', 'se.arrival_time']) }} as arrival_id,
        
        -- Keys
        se.vehicle_id,
        se.stop_id,
        se.line_id,
        se.direction_id,
        
        -- Timestamps
        se.arrival_time,
        se.arrival_timestamp,
        se.arrival_date,
        extract(hour from se.arrival_timestamp) as arrival_hour,
        extract(dow from se.arrival_timestamp) as arrival_dow,
        
        -- Target variable
        se.delay_at_arrival,
        se.delay_category,
        
        -- Event attributes
        se.dwell_time_ms,
        se.door_opened,
        se.latitude,
        se.longitude,
        
        -- Dimension attributes (denormalized for query performance)
        s.stop_name,
        s.zone_id,
        l.line_name,
        l.line_type,
        
        -- Historical features (from intermediate layer)
        sp.avg_delay_seconds as historical_avg_delay,
        sp.stddev_delay_seconds as historical_stddev_delay,
        sp.on_time_percentage as historical_on_time_pct,
        sp.arrival_count as historical_arrival_count
        
    from stop_events se
    left join stops s on se.stop_id = s.stop_id
    left join lines l on se.line_id = l.line_id
    left join stop_performance sp 
        on se.stop_id = sp.stop_id
        and se.line_id = sp.line_id
        and extract(hour from se.arrival_timestamp) = sp.hour_of_day
        and extract(dow from se.arrival_timestamp) = sp.day_of_week
)

select * from enriched