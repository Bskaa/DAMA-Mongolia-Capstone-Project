select
    sessions.session_id,
    sessions.started_at::date as date_day,
    sessions.started_at,
    sessions.ended_at,
    sessions.player_id,
    sessions.game_id,
    sessions.platform_id,
    sessions.duration_minutes,
    extract(epoch from (sessions.ended_at - sessions.started_at))::bigint
        as elapsed_seconds
from {{ ref('stg_sessions') }} as sessions
