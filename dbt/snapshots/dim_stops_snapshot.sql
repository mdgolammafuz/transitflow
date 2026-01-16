{% snapshot dim_stops_snapshot %}

{{
    config(
      target_schema='dimensions',
      unique_key='stop_id',
      strategy='check',
      check_cols=['stop_name', 'latitude', 'longitude', 'zone_id', 'stop_code'],
      invalidate_hard_deletes=True,
    )
}}

-- Pulling from the GTFS seed and aligning names/types
select 
    cast(stop_id as text) as stop_id,
    cast(stop_name as text) as stop_name,
    cast(stop_lat as double precision) as latitude,
    cast(stop_lon as double precision) as longitude,
    cast(zone_id as text) as zone_id,
    cast(stop_code as text) as stop_code
from {{ ref('seed_stops') }}

{% endsnapshot %}