-- One row per RAWG game and genre, unpacked from the original API payload.
select distinct
    games.rawg_id,
    genre ->> 'name' as genre_name,
    lower(genre ->> 'name') as genre_key,
    genre ->> 'slug' as genre_slug
from {{ ref('stg_rawg_games') }} as games
cross join lateral jsonb_array_elements(
    coalesce(games.raw_payload -> 'genres', '[]'::jsonb)
) as genre
where genre ->> 'name' is not null
