{#- Same convention as the MVP project: use the configured +schema as-is (staging, intermediate, marts). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
