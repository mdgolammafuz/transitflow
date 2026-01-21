/*
    Dimension: Lines (SCD Type 2)
    Context: Static reference data for Transit Lines.
    Hardening: Aligned with project-level variables and UTC session lock.
*/

{{ config(
    materialized='table',
    schema='dimensions'
) }}

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
        cast(line_name as text) as line_name,
        cast(line_type as text) as line_type,
        cast(operator_name as text) as operator_name,
        
        -- SCD Type 2 Metadata: Driven by the UTC-locked snapshot
        dbt_valid_from as valid_from,
        
        -- Alignment: Use the global var to ensure 'forever' is consistent across layers
        coalesce(
            dbt_valid_to, 
            cast('{{ var("scd_end_date") }}' as timestamp)
        ) as valid_to,
        
        -- Flag for the current active version of a line
        (dbt_valid_to is null) as is_current
        
    from snapshot_source
)

select * from final