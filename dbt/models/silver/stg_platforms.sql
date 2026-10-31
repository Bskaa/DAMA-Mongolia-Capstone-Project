{%- set src = source('bronze', 'platforms') -%}

select
    platform_id::integer as platform_id,
    platform_name::text as platform_name,
    manufacturer::text as manufacturer,
    {{ to_utc_timestamptz(src, 'created_at') }} as created_at,
    {{ to_utc_timestamptz(src, 'updated_at') }} as updated_at
from {{ src }}
where _sdc_deleted_at is null
