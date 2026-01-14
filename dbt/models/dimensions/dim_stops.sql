/*
    Dimension: Stops (SCD Type 2)
    Harden: Unified casting and defensive snapshot handling.
*/

-- depends_on: {{ ref('dim_stops_snapshot') }}

{{ config(materialized='table') }}

{%- set snapshot_relation = adapter.get_relation(
      database=this.database,
      schema='dimensions',
      identifier='dim_stops_snapshot') -%}

with snapshot_source as (
    {% if snapshot_relation %}
        select * from {{ ref('dim_stops_snapshot') }}
    {% else %}
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

seed_source as (
    select * from {{ ref('seed_stops') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['stop_id', 'dbt_valid_from']) }} as stop_key,
        cast(stop_id as text) as stop_id,
        stop_name,
        -- Mapping snapshot/seed columns to unified names
        cast(stop_lat as double precision) as latitude,
        cast(stop_lon as double precision) as longitude,
        zone_id,
        stop_code,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, cast('{{ var("scd_end_date") }}' as timestamp)) as valid_to,
        (dbt_valid_to is null) as is_current
    from snapshot_source
    where stop_id is not null

    union all

    select 
        {{ dbt_utils.generate_surrogate_key(['stop_id', "'1970-01-01'"]) }} as stop_key,
        cast(stop_id as text) as stop_id,
        stop_name,
        cast(stop_lat as double precision) as latitude,
        cast(stop_lon as double precision) as longitude,
        zone_id,
        stop_code,
        cast('1970-01-01' as timestamp) as valid_from,
        cast('{{ var("scd_end_date") }}' as timestamp) as valid_to,
        true as is_current
    from seed_source
    where cast(stop_id as text) not in (select coalesce(cast(stop_id as text), 'N/A') from snapshot_source)
)

select * from final