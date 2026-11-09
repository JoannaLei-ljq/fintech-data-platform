-- ISO 4217 reference data: minor_units says how many decimals amount_minor has.
select
    currency_code,
    minor_units,
    name as currency_name
from {{ source('raw', 'currencies') }}
