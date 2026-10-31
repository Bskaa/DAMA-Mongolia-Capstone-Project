{%- set src = source('bronze', 'reviews') -%}

select
    review_id::bigint as review_id,
    player_id::bigint as player_id,
    game_id::bigint as game_id,
    rating::integer as rating,
    {{ to_utc_timestamptz(src, 'review_date') }} as review_date,
    {{ to_utc_timestamptz(src, 'updated_at') }} as updated_at
from {{ src }}
where _sdc_deleted_at is null
