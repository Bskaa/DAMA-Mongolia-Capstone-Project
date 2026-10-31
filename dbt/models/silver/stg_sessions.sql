{%- set src = source('bronze', 'sessions') -%}

select
    session_id::bigint as session_id,
    player_id::bigint as player_id,
    game_id::bigint as game_id,
    platform_id::integer as platform_id,
    {{ to_utc_timestamptz(src, 'started_at') }} as started_at,
    {{ to_utc_timestamptz(src, 'ended_at') }} as ended_at,
    duration_minutes::integer as duration_minutes,
    {{ to_utc_timestamptz(src, 'updated_at') }} as updated_at
from {{ src }}
where _sdc_deleted_at is null
