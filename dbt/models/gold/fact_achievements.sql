select
    achievement_id,
    achieved_at::date as date_day,
    achieved_at,
    player_id,
    game_id,
    achievement_name
from {{ ref('stg_achievements') }}
