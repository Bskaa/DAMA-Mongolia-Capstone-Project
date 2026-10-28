select campaign_id, start_date, end_date
from {{ ref('stg_marketing_campaigns') }}
where end_date < start_date
