select
    achievement_id::bigint as achievement_id,
    player_id::bigint as player_id,
    game_id::bigint as game_id,
    achievement_name::text as achievement_name,
    achieved_at::timestamp as achieved_at,
    updated_at::timestamp as updated_at
from {{ source('raw', 'achievements') }}
where _sdc_deleted_at is null
