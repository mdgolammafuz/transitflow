-- depends_on: {{ ref('dim_stops_snapshot') }}

/*
    Dimension: Stops (SCD Type 2 + Dynamic Discovery)
    Principal Logic: Merges Snapshots (which contain Seeds) and Telemetry.
    Hardening: Removed redundant Seed union to prevent duplicate active keys.
*/

{{ config(
    materialized='table',
    schema='dimensions'
) }}

{%- set snapshot_relation = adapter.get_relation(
      database=this.database,
      schema='snapshots',
      identifier='dim_stops_snapshot') -%}

with snapshot_source as (
    {% if snapshot_relation %}
        select * from {{ ref('dim_stops_snapshot') }}
    {% else %}
        -- Fallback schema if snapshot hasn't run yet
        select 
            null::text as stop_id, 
            null::text as stop_name, 
            null::double precision as stop_lat, 
            null::double precision as stop_lon,
            null::text as zone_id,
            null::text as stop_code,
            null::timestamp as dbt_valid_from,
            null::timestamp as dbt_valid_to
        limit 0
    {% endif %}
),

telemetry_discovery as (
    -- DYNAMIC DISCOVERY: Extract unique stops seen in real telemetry
    select 
        next_stop_id as stop_id,
        avg(latitude) as discovered_lat,
        avg(longitude) as discovered_lon
    from {{ ref('stg_enriched') }}
    where next_stop_id is not null
    group by 1
),

unioned_data as (
    -- 1. Existing Snapshot History (Includes Seeds implicitly)
    select
        trim(cast(stop_id as text)) as stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        zone_id,
        stop_code,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, cast('{{ var("scd_end_date") }}' as timestamp)) as valid_to,
        (dbt_valid_to is null) as is_current
    from snapshot_source
    where stop_id is not null

    union all

    -- 2. Discovery Data (Fallback ONLY for stops truly missing from Snapshot)
    select
        trim(cast(td.stop_id as text)) as stop_id,
        'Discovered: ' || td.stop_id as stop_name,
        td.discovered_lat as stop_lat,
        td.discovered_lon as stop_lon,
        'Unmapped' as zone_id,
        td.stop_id as stop_code,
        cast('1970-01-01' as timestamp) as valid_from,
        cast('{{ var("scd_end_date") }}' as timestamp) as valid_to,
        true as is_current
    from telemetry_discovery td
    where trim(cast(td.stop_id as text)) not in (
        select coalesce(trim(cast(stop_id as text)), 'N/A') from snapshot_source
    )
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['stop_id', 'valid_from']) }} as stop_key,
        stop_id,
        cast(stop_name as text) as stop_name,
        cast(stop_lat as double precision) as stop_lat,
        cast(stop_lon as double precision) as stop_lon,
        cast(zone_id as text) as zone_id,
        cast(stop_code as text) as stop_code,
        valid_from,
        valid_to,
        is_current
    from unioned_data
)

select * from final