{#- on-run-start: one row describing this build, written before any model runs.
    sync_run_id is the sync run whose committed raw data the models will read; publication
    refuses if any other sync run commits before it publishes. -#}
{% macro record_build_manifest() %}
create schema if not exists marts_candidate;
create table if not exists marts_candidate.mart_build_info (
    pipeline_run_id   text        not null,
    dbt_invocation_id text        not null,
    sync_run_id       bigint      not null,
    sync_watermark    text        not null,
    source_generation uuid        not null,
    built_at          timestamptz not null
);
delete from marts_candidate.mart_build_info;
insert into marts_candidate.mart_build_info
select '{{ var("v2_pipeline_run_id") }}', '{{ invocation_id }}', last_run_id, watermark::text, source_generation,
       clock_timestamp()
from meta.sync_state
where source_name = 'v2_oltp' and status = 'active'
{% endmacro %}
