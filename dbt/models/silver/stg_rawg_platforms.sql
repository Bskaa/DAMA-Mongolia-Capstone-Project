select
    rawg_id::bigint as rawg_id,
    name::text as name,
    slug::text as slug,
    games_count::integer as games_count,
    image_background::text as image_background,
    image::text as image,
    year_start::integer as year_start,
    year_end::integer as year_end,
    raw_payload::jsonb as raw_payload,
    fetched_at::timestamptz as fetched_at
from {{ source('bronze', 'rawg_platforms') }}
