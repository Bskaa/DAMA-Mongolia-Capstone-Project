select
    platform_id::integer as platform_id,
    platform_name::text as platform_name,
    manufacturer::text as manufacturer,
    created_at::timestamp as created_at,
    updated_at::timestamp as updated_at
from {{ source('bronze', 'platforms') }}
where _sdc_deleted_at is null
