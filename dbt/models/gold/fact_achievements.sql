select
    achievement_id,
    {{ date_key(reporting_date('achieved_at')) }} as date_key,
    {{ reporting_date('achieved_at') }} as date_day,
    achieved_at,
    player_id,
    game_id,
    achievement_name
from {{ ref('stg_achievements') }}
