{% snapshot dim_stops_snapshot %}

{{
    config(
      target_schema='dimensions',
      unique_key='stop_id',
      strategy='check',
      check_cols=['stop_name', 'stop_lat', 'stop_lon', 'zone_id', 'stop_code'],
      invalidate_hard_deletes=True,
    )
}}

-- Pulling directly from the GTFS seed
select 
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    zone_id,
    stop_code
from {{ ref('seed_stops') }}

{% endsnapshot %}