/*
    Override default schema name generation.
    
    By default, dbt creates schemas like: target_schema_custom_schema
    This macro keeps just the custom schema name.
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}