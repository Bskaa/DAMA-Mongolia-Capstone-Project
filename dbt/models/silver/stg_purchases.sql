select
    purchase_id::bigint as purchase_id,
    player_id::bigint as player_id,
    game_id::bigint as game_id,
    purchase_date::timestamp as purchase_date,
    amount::numeric(10, 2) as amount,
    currency::text as currency,
    payment_method::text as payment_method,
    updated_at::timestamp as updated_at
from {{ source('bronze', 'purchases') }}
where _sdc_deleted_at is null
