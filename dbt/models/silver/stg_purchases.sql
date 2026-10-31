{%- set src = source('bronze', 'purchases') -%}

select
    purchase_id::bigint as purchase_id,
    player_id::bigint as player_id,
    game_id::bigint as game_id,
    {{ to_utc_timestamptz(src, 'purchase_date') }} as purchase_date,
    amount::numeric(10, 2) as amount,
    currency::text as currency,
    payment_method::text as payment_method,
    {{ to_utc_timestamptz(src, 'updated_at') }} as updated_at
from {{ src }}
where _sdc_deleted_at is null
