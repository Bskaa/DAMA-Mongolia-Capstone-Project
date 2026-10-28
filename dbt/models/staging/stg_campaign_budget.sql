select
    campaign_id::bigint as campaign_id,
    budget::numeric(12, 2) as budget,
    actual_spend::numeric(12, 2) as actual_spend,
    source_file::text as source_file,
    loaded_at::timestamptz as loaded_at
from {{ source('raw', 'raw_campaign_budget') }}
