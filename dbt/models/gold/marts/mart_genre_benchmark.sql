-- Benchmarks our games against the external RAWG catalogue, by genre.
-- RAWG and the synthetic catalogue share no game key, so genre is the
-- (documented) conformed attribute used to compare them.
with our_games as (
    select
        lower(games.genre) as genre_key,
        min(games.genre) as genre,
        count(distinct games.game_id) as our_games,
        avg(games.price) as our_avg_price
    from {{ ref('dim_games') }} as games
    group by lower(games.genre)
),
our_ratings as (
    select
        lower(games.genre) as genre_key,
        avg(reviews.rating) as our_avg_rating,
        count(*) as our_reviews
    from {{ ref('fact_reviews') }} as reviews
    join {{ ref('dim_games') }} as games using (game_id)
    group by lower(games.genre)
),
rawg as (
    select
        genres.genre_key,
        count(distinct rawg_games.rawg_id) as rawg_games,
        avg(rawg_games.rating) filter (where rawg_games.ratings_count > 0) as rawg_avg_rating,
        avg(rawg_games.metacritic) as rawg_avg_metacritic,
        avg(rawg_games.playtime) as rawg_avg_playtime_hours
    from {{ ref('stg_rawg_game_genres') }} as genres
    join {{ ref('stg_rawg_games') }} as rawg_games using (rawg_id)
    group by genres.genre_key
)
select
    our_games.genre,
    our_games.our_games,
    round(our_games.our_avg_price, 2) as our_avg_price,
    coalesce(our_ratings.our_reviews, 0) as our_reviews,
    round(our_ratings.our_avg_rating, 2) as our_avg_rating,
    coalesce(rawg.rawg_games, 0) as rawg_games,
    round(rawg.rawg_avg_rating, 2) as rawg_avg_rating,
    round(rawg.rawg_avg_metacritic, 1) as rawg_avg_metacritic,
    round(rawg.rawg_avg_playtime_hours, 1) as rawg_avg_playtime_hours,
    round(our_ratings.our_avg_rating - rawg.rawg_avg_rating, 2) as rating_gap_vs_rawg
from our_games
left join our_ratings using (genre_key)
left join rawg using (genre_key)
