-- Actual performance of each game against the targets from game_targets.xlsx.
-- Assumption (documented in README): targets apply to all activity in the
-- warehouse, because the workbook has no target period column.
with sessions as (
    select
        game_id,
        count(*) as actual_sessions,
        count(distinct player_id) as actual_players
    from {{ ref('fact_sessions') }}
    group by game_id
),
revenue as (
    select game_id, sum(amount_usd) as actual_revenue_usd
    from {{ ref('fact_purchases') }}
    group by game_id
),
ratings as (
    select game_id, avg(rating) as actual_rating, count(*) as reviews
    from {{ ref('fact_reviews') }}
    group by game_id
)
select
    games.game_id,
    games.game_name,
    games.genre,
    games.target_revenue,
    coalesce(revenue.actual_revenue_usd, 0)::numeric(14, 2) as actual_revenue_usd,
    round(coalesce(revenue.actual_revenue_usd, 0) / nullif(games.target_revenue, 0), 4)
        as revenue_attainment,
    games.target_players,
    coalesce(sessions.actual_players, 0) as actual_players,
    round(coalesce(sessions.actual_players, 0)::numeric / nullif(games.target_players, 0), 4)
        as players_attainment,
    games.target_sessions,
    coalesce(sessions.actual_sessions, 0) as actual_sessions,
    round(coalesce(sessions.actual_sessions, 0)::numeric / nullif(games.target_sessions, 0), 4)
        as sessions_attainment,
    games.target_rating,
    round(ratings.actual_rating, 2) as actual_rating,
    coalesce(ratings.reviews, 0) as reviews,
    round(ratings.actual_rating - games.target_rating, 2) as rating_gap
from {{ ref('dim_games') }} as games
left join sessions using (game_id)
left join revenue using (game_id)
left join ratings using (game_id)
where games.target_revenue is not null
