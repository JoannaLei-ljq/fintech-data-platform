select
    user_id,
    email,
    full_name,
    country     as country_code,
    created_at,
    updated_at
from {{ source('raw', 'users') }}
