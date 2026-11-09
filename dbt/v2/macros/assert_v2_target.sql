{#- Guard: V2 models are only ever built in a v2_* database through the v2 target.
    The profile already maps any other database name to a non-existent one, so dbt cannot even
    connect elsewhere; this second check fails loudly with an explanation. -#}
{% macro assert_v2_target() %}
    {%- if target.name != 'v2' or not target.dbname.startswith('v2_') -%}
        {{ exceptions.raise_compiler_error(
            "V2 models may only run with target 'v2' against a v2_* database, got target '"
            ~ target.name ~ "' / database '" ~ target.dbname ~ "'") }}
    {%- endif -%}
    select 1
{% endmacro %}
