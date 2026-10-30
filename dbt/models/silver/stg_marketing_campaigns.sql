select
    campaign_id::bigint as campaign_id,
    game_id::bigint as game_id,
    campaign::text as campaign,
    budget::numeric(12, 2) as budget,
    start_date::date as start_date,
    end_date::date as end_date,
    source_file::text as source_file,
    loaded_at::timestamptz as loaded_at
from {{ source('bronze', 'raw_marketing_campaigns') }}
