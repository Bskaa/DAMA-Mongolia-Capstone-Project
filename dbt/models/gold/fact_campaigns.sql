-- Left join: a campaign without a budget row must stay visible (and is caught
-- by the not_null test on actual_spend) instead of disappearing silently.
select
    campaigns.campaign_id,
    campaigns.game_id,
    campaigns.campaign,
    campaigns.start_date,
    campaigns.end_date,
    {{ date_key('campaigns.start_date') }} as start_date_key,
    {{ date_key('campaigns.end_date') }} as end_date_key,
    (campaigns.end_date - campaigns.start_date + 1) as campaign_days,
    campaigns.budget,
    budgets.actual_spend,
    (campaigns.budget - budgets.actual_spend)::numeric(12, 2) as remaining_budget,
    round(budgets.actual_spend / nullif(campaigns.budget, 0), 4)::numeric(10, 4) as budget_spend_ratio
from {{ ref('stg_marketing_campaigns') }} as campaigns
left join {{ ref('stg_campaign_budget') }} as budgets using (campaign_id)
