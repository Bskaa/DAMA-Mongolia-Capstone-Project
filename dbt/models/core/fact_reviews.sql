select
    review_id,
    review_date::date as date_day,
    review_date,
    player_id,
    game_id,
    rating
from {{ ref('stg_reviews') }}
