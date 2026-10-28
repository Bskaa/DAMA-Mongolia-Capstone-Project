select
    sessions.session_id,
    sessions.game_id,
    sessions.platform_id
from {{ ref('fact_sessions') }} as sessions
left join {{ ref('bridge_game_platforms') }} as bridge
    on sessions.game_id = bridge.game_id
    and sessions.platform_id = bridge.platform_id
where bridge.game_id is null
