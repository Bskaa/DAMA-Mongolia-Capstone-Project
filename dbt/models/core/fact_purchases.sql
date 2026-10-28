select
    purchase_id,
    purchase_date::date as date_day,
    purchase_date,
    player_id,
    game_id,
    amount,
    currency,
    payment_method
from {{ ref('stg_purchases') }}
