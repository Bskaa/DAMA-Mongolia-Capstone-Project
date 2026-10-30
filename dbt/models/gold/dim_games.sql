select
    games.game_id,
    games.game_name,
    games.developer,
    games.publisher,
    games.release_date,
    games.genre,
    games.price,
    targets.target_revenue,
    targets.target_players,
    targets.target_sessions,
    targets.target_rating
from {{ ref('stg_games') }} as games
left join {{ ref('stg_game_targets') }} as targets using (game_id)
