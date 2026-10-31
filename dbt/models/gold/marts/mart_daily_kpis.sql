-- One row per calendar day: engagement and monetisation KPIs in USD.
with activity_bounds as (
    select min(date_day) as first_day, max(date_day) as last_day
    from (
        select date_day from {{ ref('fact_sessions') }}
        union all
        select date_day from {{ ref('fact_purchases') }}
    ) as activity
),
days as (
    select dim_date.date_key, dim_date.date_day
    from {{ ref('dim_date') }} as dim_date
    cross join activity_bounds
    where dim_date.date_day between activity_bounds.first_day and activity_bounds.last_day
),
sessions as (
    select
        date_key,
        count(*) as sessions,
        count(distinct player_id) as daily_active_players,
        sum(duration_minutes) as play_minutes
    from {{ ref('fact_sessions') }}
    group by date_key
),
purchases as (
    select
        date_key,
        count(*) as purchases,
        count(distinct player_id) as paying_players,
        sum(amount_usd) as revenue_usd
    from {{ ref('fact_purchases') }}
    group by date_key
),
first_seen as (
    select player_id, min(date_key) as first_date_key
    from {{ ref('fact_sessions') }}
    group by player_id
),
new_players as (
    select first_date_key as date_key, count(*) as new_players
    from first_seen
    group by first_date_key
)
select
    days.date_key::integer as date_key,
    days.date_day::date as date_day,
    coalesce(sessions.sessions, 0)::bigint as sessions,
    coalesce(sessions.daily_active_players, 0)::bigint as daily_active_players,
    coalesce(new_players.new_players, 0)::bigint as new_players,
    coalesce(sessions.play_minutes, 0)::bigint as play_minutes,
    coalesce(purchases.purchases, 0)::bigint as purchases,
    coalesce(purchases.paying_players, 0)::bigint as paying_players,
    coalesce(purchases.revenue_usd, 0)::numeric(14, 2) as revenue_usd,
    round(
        coalesce(purchases.revenue_usd, 0)
            / nullif(sessions.daily_active_players, 0),
        4
    )::numeric(14, 4) as arpdau_usd,
    round(purchases.revenue_usd / nullif(purchases.paying_players, 0), 4)::numeric(14, 4)
        as arppu_usd
from days
left join sessions using (date_key)
left join purchases using (date_key)
left join new_players using (date_key)
