select
    user_id,
    email,
    full_name,
    country,
    created_at,
    updated_at
from {{ source('raw', 'users') }}
