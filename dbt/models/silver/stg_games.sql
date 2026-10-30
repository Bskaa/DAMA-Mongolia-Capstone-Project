select
    game_id::bigint as game_id,
    game_name::text as game_name,
    developer::text as developer,
    publisher::text as publisher,
    release_date::date as release_date,
    genre::text as genre,
    price::numeric(10, 2) as price,
    created_at::timestamp as created_at,
    updated_at::timestamp as updated_at
from {{ source('bronze', 'games') }}
where _sdc_deleted_at is null
