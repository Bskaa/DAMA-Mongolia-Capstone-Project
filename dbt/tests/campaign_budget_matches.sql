select
    campaigns.campaign_id,
    campaigns.budget as campaign_budget,
    budgets.budget as budget_workbook_budget
from {{ ref('stg_marketing_campaigns') }} as campaigns
join {{ ref('stg_campaign_budget') }} as budgets using (campaign_id)
where campaigns.budget is distinct from budgets.budget
