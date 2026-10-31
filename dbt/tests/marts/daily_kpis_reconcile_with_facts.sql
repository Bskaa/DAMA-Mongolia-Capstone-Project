-- Consistency: the daily mart must reconcile exactly with the underlying facts.
with mart as (
    select sum(sessions) as sessions, sum(purchases) as purchases, sum(revenue_usd) as revenue_usd
    from {{ ref('mart_daily_kpis') }}
),
facts as (
    select
        (select count(*) from {{ ref('fact_sessions') }}) as sessions,
        (select count(*) from {{ ref('fact_purchases') }}) as purchases,
        (select coalesce(sum(amount_usd), 0) from {{ ref('fact_purchases') }}) as revenue_usd
)
select mart.sessions, facts.sessions, mart.purchases, facts.purchases, mart.revenue_usd, facts.revenue_usd
from mart cross join facts
where mart.sessions <> facts.sessions
    or mart.purchases <> facts.purchases
    or abs(mart.revenue_usd - facts.revenue_usd) > 0.01
