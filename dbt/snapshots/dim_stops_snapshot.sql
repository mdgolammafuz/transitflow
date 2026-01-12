{% snapshot dim_stops_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='stop_id',
      strategy='check',
      check_cols=['latitude', 'longitude'],
    )
}}

-- CHANGE THIS LINE:
select * from {{ ref('stg_stop_events') }} 

{% endsnapshot %}