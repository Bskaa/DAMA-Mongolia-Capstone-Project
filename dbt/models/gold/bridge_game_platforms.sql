select
    game_id,
    platform_id,
    created_at,
    updated_at
from {{ ref('stg_game_platforms') }}
