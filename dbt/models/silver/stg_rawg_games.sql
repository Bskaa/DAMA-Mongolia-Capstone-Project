select
    rawg_id::bigint as rawg_id,
    name::text as name,
    slug::text as slug,
    released::date as released,
    updated::timestamptz as updated_at,
    rating::numeric(6, 3) as rating,
    rating_top::integer as rating_top,
    ratings_count::integer as ratings_count,
    metacritic::integer as metacritic,
    playtime::integer as playtime,
    background_image::text as background_image,
    raw_payload::jsonb as raw_payload,
    fetched_at::timestamptz as fetched_at
from {{ source('bronze', 'rawg_games') }}
