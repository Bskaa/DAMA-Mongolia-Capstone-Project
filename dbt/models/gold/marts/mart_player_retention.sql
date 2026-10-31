-- Monthly cohort retention: players grouped by the month of their first
-- session, and the share of each cohort that played again N months later.
with activity as (
    select distinct
        player_id,
        date_trunc('month', date_day)::date as activity_month
    from {{ ref('fact_sessions') }}
),
cohorts as (
    select player_id, min(activity_month) as cohort_month
    from activity
    group by player_id
),
cohort_sizes as (
    select cohort_month, count(*) as cohort_players
    from cohorts
    group by cohort_month
),
retained as (
    select
        cohorts.cohort_month,
        (
            (extract(year from activity.activity_month) - extract(year from cohorts.cohort_month)) * 12
            + extract(month from activity.activity_month) - extract(month from cohorts.cohort_month)
        )::integer as months_since_first_session,
        count(distinct activity.player_id) as active_players
    from activity
    join cohorts using (player_id)
    group by 1, 2
)
select
    {{ date_key('retained.cohort_month') }} as cohort_month_key,
    retained.cohort_month,
    retained.months_since_first_session,
    cohort_sizes.cohort_players,
    retained.active_players,
    round(retained.active_players::numeric / cohort_sizes.cohort_players, 4) as retention_rate
from retained
join cohort_sizes using (cohort_month)
