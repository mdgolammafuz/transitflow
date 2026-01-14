/*
    Macro: generate_schema_name
    Pattern: Strict Schema Naming
    This override ensures that dbt uses the exact schema names defined in 
    dbt_project.yml without prefixing them with the target schema.
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}