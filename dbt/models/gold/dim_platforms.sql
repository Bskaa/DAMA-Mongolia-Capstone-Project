select
    platform_id,
    platform_name,
    manufacturer
from {{ ref('stg_platforms') }}
