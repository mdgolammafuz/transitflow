/*
    Dimension: Lines (SCD Type 2)
    
    Tracks changes to transit line attributes over time.
*/

with seed_lines as (
    select * from {{ ref('seed_lines') }}
),

current_lines as (
    select
        line_id,
        line_name,
        line_type,
        operator_name,
        -- SCD Type 2 fields
        current_date as valid_from,
        cast('{{ var("scd_end_date") }}' as date) as valid_to,
        true as is_current
    from seed_lines
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['line_id', 'valid_from']) }} as line_key,
        line_id,
        line_name,
        line_type,
        operator_name,
        valid_from,
        valid_to,
        is_current
    from current_lines
)

select * from final