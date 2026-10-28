select game_id, target_revenue, target_players, target_sessions, target_rating
from {{ ref('stg_game_targets') }}
where target_revenue < 0
    or target_players < 0
    or target_sessions < 0
    or target_rating < 0
    or target_rating > 5
