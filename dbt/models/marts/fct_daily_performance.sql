/*
    Fact Table: Daily Performance
    Aggregated KPIs for historical line analysis.
*/

{{ config(
    materialized='table',
    schema='marts'
) }}

with raw_daily as (
    -- Reading from the Spark Sink in public schema
    select * from {{ source('raw_lakehouse', 'fct_daily_performance') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['line_id', 'date']) }} as metric_id,
        line_id,
        cast(date as date) as metric_date,
        
        -- Aggregates cast to match enforced contract (Postgres double precision/bigint)
        cast(1 as bigint) as total_journeys, 
        cast(total_events as bigint) as total_events,
        cast(0 as bigint) as unique_vehicles, 
        
        cast(avg_delay_seconds as double precision) as avg_delay_seconds,
        cast(on_time_percentage as double precision) as on_time_percentage,
        cast(0.0 as double precision) as total_distance_km
        
    from raw_daily
)

select * from final