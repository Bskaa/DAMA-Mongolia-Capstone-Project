select
    campaigns.campaign_id,
    campaigns.game_id,
    campaigns.campaign,
    campaigns.start_date,
    campaigns.end_date,
    campaigns.budget,
    budgets.actual_spend,
    campaigns.budget - budgets.actual_spend as remaining_budget,
    budgets.actual_spend / nullif(campaigns.budget, 0) as budget_spend_ratio
from {{ ref('stg_marketing_campaigns') }} as campaigns
inner join {{ ref('stg_campaign_budget') }} as budgets using (campaign_id)
