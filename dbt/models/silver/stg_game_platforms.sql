{%- set src = source('bronze', 'game_platforms') -%}

select
    game_id::bigint as game_id,
    platform_id::integer as platform_id,
    {{ to_utc_timestamptz(src, 'created_at') }} as created_at,
    {{ to_utc_timestamptz(src, 'updated_at') }} as updated_at
from {{ src }}
where _sdc_deleted_at is null
