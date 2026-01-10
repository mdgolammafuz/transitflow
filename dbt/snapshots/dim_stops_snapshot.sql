{% snapshot dim_stops_snapshot %}

{{
    config(
      target_schema='dimensions',
      unique_key='stop_id',
      strategy='check',
      check_cols=['stop_name', 'latitude', 'longitude'],
      invalidate_hard_deletes=True,
    )
}}

select * from {{ ref('stg_stops') }}

{% endsnapshot %}