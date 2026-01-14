/*
    Dimension: Lines (SCD Type 2)
    Harden: Ensures type consistency for surrogate keys and SCD dates.
*/

{{ config(materialized='table') }}

with snapshot_source as (
    -- Reads from the snapshot which tracks changes in the seed_lines source
    select * from {{ ref('dim_lines_snapshot') }}
),

final as (
    select
        -- Surrogate key for joining with fact tables
        {{ dbt_utils.generate_surrogate_key(['line_id', 'dbt_valid_from']) }} as line_key,
        
        -- Business logic columns
        cast(line_id as text) as line_id,
        line_name,
        line_type,
        operator_name,
        
        -- SCD Type 2 Metadata
        dbt_valid_from as valid_from,
        -- Uses the global variable for end-of-time (usually 2099-01-01)
        coalesce(
            dbt_valid_to, 
            cast('{{ var("scd_end_date") }}' as timestamp)
        ) as valid_to,
        
        -- Flag for the current active version of a line
        (dbt_valid_to is null) as is_current
        
    from snapshot_source
)

select * from final