select
    game_id::bigint as game_id,
    target_revenue::numeric(12, 2) as target_revenue,
    target_players::bigint as target_players,
    target_sessions::bigint as target_sessions,
    target_rating::numeric(4, 2) as target_rating,
    source_file::text as source_file,
    loaded_at::timestamptz as loaded_at
from {{ source('raw', 'raw_game_targets') }}
