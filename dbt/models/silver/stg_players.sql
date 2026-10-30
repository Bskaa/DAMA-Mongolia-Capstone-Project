select
    player_id::bigint as player_id,
    username::text as username,
    country::text as country,
    birth_year::integer as birth_year,
    created_at::timestamp as created_at,
    updated_at::timestamp as updated_at
from {{ source('bronze', 'players') }}
where _sdc_deleted_at is null
