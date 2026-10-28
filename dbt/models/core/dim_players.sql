select
    player_id,
    username,
    country,
    birth_year,
    created_at,
    updated_at
from {{ ref('stg_players') }}
