select
    merchant_id,
    merchant_name,
    category,
    country,
    created_at,
    updated_at
from {{ source('raw', 'merchants') }}
