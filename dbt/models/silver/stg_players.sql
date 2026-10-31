{%- set src = source('bronze', 'players') -%}

select
    player_id::bigint as player_id,
    username::text as username,
    country::text as country,
    birth_year::integer as birth_year,
    {{ to_utc_timestamptz(src, 'created_at') }} as created_at,
    {{ to_utc_timestamptz(src, 'updated_at') }} as updated_at
from {{ src }}
where _sdc_deleted_at is null
