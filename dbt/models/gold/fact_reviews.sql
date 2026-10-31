select
    review_id,
    {{ date_key(reporting_date('review_date')) }} as date_key,
    {{ reporting_date('review_date') }} as date_day,
    review_date,
    player_id,
    game_id,
    rating
from {{ ref('stg_reviews') }}
