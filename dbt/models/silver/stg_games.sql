{%- set src = source('bronze', 'games') -%}

select
    game_id::bigint as game_id,
    game_name::text as game_name,
    developer::text as developer,
    publisher::text as publisher,
    release_date::date as release_date,
    genre::text as genre,
    price::numeric(10, 2) as price,
    {{ to_utc_timestamptz(src, 'created_at') }} as created_at,
    {{ to_utc_timestamptz(src, 'updated_at') }} as updated_at
from {{ src }}
where _sdc_deleted_at is null
