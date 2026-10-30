select
    review_id::bigint as review_id,
    player_id::bigint as player_id,
    game_id::bigint as game_id,
    rating::integer as rating,
    review_date::timestamp as review_date,
    updated_at::timestamp as updated_at
from {{ source('bronze', 'reviews') }}
where _sdc_deleted_at is null
