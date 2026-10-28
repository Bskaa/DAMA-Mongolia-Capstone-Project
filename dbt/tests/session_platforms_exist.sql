select
    sessions.session_id,
    sessions.game_id,
    sessions.platform_id
from {{ ref('stg_sessions') }} as sessions
left join {{ ref('stg_game_platforms') }} as game_platforms
    on sessions.game_id = game_platforms.game_id
    and sessions.platform_id = game_platforms.platform_id
where game_platforms.game_id is null
