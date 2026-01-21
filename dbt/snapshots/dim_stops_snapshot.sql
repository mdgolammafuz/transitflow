{% snapshot dim_stops_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='stop_id',
    strategy='check',
    check_cols=['stop_name', 'stop_lat', 'stop_lon', 'zone_id', 'stop_code'],
    invalidate_hard_deletes=True
) }}

-- Pulling from the GTFS seed. 
-- UTC session lock from profiles.yml ensures valid_from is accurate.
select 
    cast(stop_id as text) as stop_id,
    cast(stop_name as text) as stop_name,
    cast(stop_lat as double precision) as stop_lat,
    cast(stop_lon as double precision) as stop_lon,
    cast(zone_id as text) as zone_id,
    cast(stop_code as text) as stop_code
from {{ ref('seed_stops') }}

{% endsnapshot %}