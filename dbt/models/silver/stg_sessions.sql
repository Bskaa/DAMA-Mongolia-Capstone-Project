select
    session_id::bigint as session_id,
    player_id::bigint as player_id,
    game_id::bigint as game_id,
    platform_id::integer as platform_id,
    started_at::timestamp as started_at,
    ended_at::timestamp as ended_at,
    duration_minutes::integer as duration_minutes,
    updated_at::timestamp as updated_at
from {{ source('bronze', 'sessions') }}
where _sdc_deleted_at is null
