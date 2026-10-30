select
    game_id::bigint as game_id,
    platform_id::integer as platform_id,
    created_at::timestamp as created_at,
    updated_at::timestamp as updated_at
from {{ source('bronze', 'game_platforms') }}
where _sdc_deleted_at is null
