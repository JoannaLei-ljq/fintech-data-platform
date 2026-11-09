select
    merchant_id,
    merchant_name,
    category,
    country     as country_code,
    created_at,
    updated_at
from {{ source('raw', 'merchants') }}
