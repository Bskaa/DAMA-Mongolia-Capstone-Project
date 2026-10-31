{%- set src = source('bronze', 'achievements') -%}

select
    achievement_id::bigint as achievement_id,
    player_id::bigint as player_id,
    game_id::bigint as game_id,
    achievement_name::text as achievement_name,
    {{ to_utc_timestamptz(src, 'achieved_at') }} as achieved_at,
    {{ to_utc_timestamptz(src, 'updated_at') }} as updated_at
from {{ src }}
where _sdc_deleted_at is null
