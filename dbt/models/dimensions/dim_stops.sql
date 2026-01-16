/*
    Dimension: Stops (SCD Type 2)
    Context: Static reference data for Stops.
    Methodical: Updated to use 'latitude/longitude' to match the new snapshot schema.
*/

-- depends_on: {{ ref('dim_stops_snapshot') }}

{{ config(
    materialized='table',
    schema='dimensions'
) }}

{%- set snapshot_relation = adapter.get_relation(
      database=this.database,
      schema='dimensions',
      identifier='dim_stops_snapshot') -%}

with snapshot_source as (
    {% if snapshot_relation %}
        select * from {{ ref('dim_stops_snapshot') }}
    {% else %}
        -- Fallback schema must match the snapshot's output column names
        select 
            null::text as stop_id, 
            null::text as stop_name, 
            null::double precision as latitude, 
            null::double precision as longitude,
            null::text as zone_id,
            null::text as stop_code,
            null::timestamp as dbt_valid_from,
            null::timestamp as dbt_valid_to
        limit 0
    {% endif %}
),

seed_source as (
    -- Primary source for GTFS reference data
    select * from {{ ref('seed_stops') }}
),

final as (
    -- 1. Historical data from Snapshots (Names already aligned in snapshot file)
    select
        {{ dbt_utils.generate_surrogate_key(['stop_id', 'dbt_valid_from']) }} as stop_key,
        cast(stop_id as text) as stop_id,
        cast(stop_name as text) as stop_name,
        cast(latitude as double precision) as latitude,
        cast(longitude as double precision) as longitude,
        cast(zone_id as text) as zone_id,
        cast(stop_code as text) as stop_code,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, cast('2099-12-31' as timestamp)) as valid_to,
        (dbt_valid_to is null) as is_current
    from snapshot_source
    where stop_id is not null

    union all

    -- 2. Current seed data not yet captured by snapshots
    select 
        {{ dbt_utils.generate_surrogate_key(['stop_id', "'1970-01-01'"]) }} as stop_key,
        cast(stop_id as text) as stop_id,
        cast(stop_name as text) as stop_name,
        cast(stop_lat as double precision) as latitude,
        cast(stop_lon as double precision) as longitude,
        cast(zone_id as text) as zone_id,
        cast(stop_code as text) as stop_code,
        cast('1970-01-01' as timestamp) as valid_from,
        cast('2099-12-31' as timestamp) as valid_to,
        true as is_current
    from seed_source
    where cast(stop_id as text) not in (
        select coalesce(cast(stop_id as text), 'N/A') from snapshot_source
    )
)

select * from final