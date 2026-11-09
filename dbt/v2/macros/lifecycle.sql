{#- The five legal payment lifecycle paths (Phase 1), as comma-joined status sequences. -#}
{% macro legal_lifecycle_paths() -%}
    ('created', 'created,authorized', 'created,authorized,captured', 'created,failed', 'created,authorized,failed')
{%- endmacro %}

{#- Safe percentage: NULL when the denominator is zero, rounded to 2 decimals. -#}
{% macro safe_pct(numerator, denominator) -%}
    case when ({{ denominator }}) = 0 then null
         else round(100.0 * ({{ numerator }}) / ({{ denominator }}), 2) end::numeric(5, 2)
{%- endmacro %}

{#- UTC calendar date of a timestamptz. -#}
{% macro utc_date(column) -%}
    ({{ column }} at time zone 'UTC')::date
{%- endmacro %}
