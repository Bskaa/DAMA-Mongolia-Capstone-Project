select
    rawg_id::bigint as rawg_id,
    name::text as name,
    slug::text as slug,
    games_count::integer as games_count,
    image_background::text as image_background,
    raw_payload::jsonb as raw_payload,
    fetched_at::timestamptz as fetched_at
from {{ source('bronze', 'rawg_genres') }}
