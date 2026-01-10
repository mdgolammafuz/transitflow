{% snapshot dim_lines_snapshot %}

{{
    config(
      target_schema='dimensions',
      unique_key='line_id',
      strategy='check',
      check_cols=['line_name', 'line_type', 'operator_name'],
      invalidate_hard_deletes=True,
    )
}}

-- Pulling directly from the seed as the initial source for history tracking
select * from {{ ref('seed_lines') }}

{% endsnapshot %}