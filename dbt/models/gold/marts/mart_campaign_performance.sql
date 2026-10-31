-- Campaign effectiveness: revenue of the promoted game during the campaign
-- compared with an equally long window immediately before it.
with campaigns as (
    select * from {{ ref('fact_campaigns') }}
),
purchases as (
    select game_id, date_day, amount_usd
    from {{ ref('fact_purchases') }}
),
windows as (
    select
        campaigns.campaign_id,
        sum(purchases.amount_usd) filter (
            where purchases.date_day between campaigns.start_date and campaigns.end_date
        ) as campaign_revenue_usd,
        sum(purchases.amount_usd) filter (
            where purchases.date_day between campaigns.start_date - campaigns.campaign_days
                and campaigns.start_date - 1
        ) as baseline_revenue_usd
    from campaigns
    left join purchases
        on purchases.game_id = campaigns.game_id
        and purchases.date_day between campaigns.start_date - campaigns.campaign_days
            and campaigns.end_date
    group by campaigns.campaign_id
)
select
    campaigns.campaign_id,
    campaigns.campaign,
    campaigns.game_id,
    games.game_name,
    campaigns.start_date,
    campaigns.end_date,
    campaigns.campaign_days,
    campaigns.budget,
    campaigns.actual_spend,
    campaigns.budget_spend_ratio,
    coalesce(windows.campaign_revenue_usd, 0)::numeric(14, 2) as campaign_revenue_usd,
    coalesce(windows.baseline_revenue_usd, 0)::numeric(14, 2) as baseline_revenue_usd,
    (coalesce(windows.campaign_revenue_usd, 0) - coalesce(windows.baseline_revenue_usd, 0))::numeric(14, 2)
        as revenue_uplift_usd,
    round(
        (coalesce(windows.campaign_revenue_usd, 0) - coalesce(windows.baseline_revenue_usd, 0))
            / nullif(campaigns.actual_spend, 0),
        4
    ) as uplift_roi,
    campaigns.end_date < current_date as is_finished
from campaigns
left join windows using (campaign_id)
left join {{ ref('dim_games') }} as games using (game_id)
