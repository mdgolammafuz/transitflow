/*
    Dimension: Lines (SCD Type 2)
*/

with snapshot_source as (
    -- Logic depends on creating dbt/snapshots/dim_lines_snapshot.sql
    select * from {{ ref('dim_lines_snapshot') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['line_id', 'dbt_valid_from']) }} as line_key,
        line_id,
        line_name,
        line_type,
        operator_name,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, cast('{{ var("scd_end_date") }}' as date)) as valid_to,
        (dbt_valid_to is null) as is_current
    from snapshot_source
)

select * from final