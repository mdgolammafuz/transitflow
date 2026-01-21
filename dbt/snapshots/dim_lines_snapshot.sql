{% snapshot dim_lines_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='line_id',
      strategy='check',
      check_cols=['line_name', 'line_type', 'operator_name'],
      invalidate_hard_deletes=True,
    )
}}

select 
    cast(line_id as text) as line_id,
    cast(line_name as text) as line_name,
    cast(line_type as text) as line_type,
    cast(operator_name as text) as operator_name
from {{ ref('seed_lines') }}

{% endsnapshot %}