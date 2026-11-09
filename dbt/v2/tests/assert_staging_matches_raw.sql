-- Staging stays 1:1 with raw: no filtered, duplicated or dropped rows.
{% set pairs = [('currencies', 'stg_currencies'), ('users', 'stg_users'), ('merchants', 'stg_merchants'),
                ('transactions', 'stg_transactions'), ('payment_events', 'stg_payment_events'),
                ('refunds', 'stg_refunds')] %}
{% for raw_table, stg in pairs %}
select '{{ raw_table }}' as table_name,
       (select count(*) from {{ source('raw', raw_table) }}) as raw_rows,
       (select count(*) from {{ ref(stg) }})                 as staging_rows
where (select count(*) from {{ source('raw', raw_table) }}) <> (select count(*) from {{ ref(stg) }})
{% if not loop.last %}union all{% endif %}
{% endfor %}
