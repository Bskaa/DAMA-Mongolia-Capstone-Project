{{
    config(
        materialized='incremental',
        unique_key='purchase_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

-- Amounts stay in the original currency; amount_usd uses the illustrative
-- fx_rates_to_usd seed and is only for cross-currency comparisons.
select
    purchases.purchase_id,
    {{ date_key(reporting_date('purchases.purchase_date')) }} as date_key,
    {{ reporting_date('purchases.purchase_date') }} as date_day,
    purchases.purchase_date,
    purchases.player_id,
    purchases.game_id,
    purchases.amount,
    purchases.currency,
    round(purchases.amount * fx.usd_per_unit, 2)::numeric(14, 2) as amount_usd,
    purchases.payment_method,
    purchases.updated_at
from {{ ref('stg_purchases') }} as purchases
left join {{ ref('fx_rates_to_usd') }} as fx
    on purchases.currency = fx.currency
{% if is_incremental() %}
    {%- set existing_columns = adapter.get_columns_in_relation(this) | map(attribute='name') | list -%}
    {%- if 'updated_at' in existing_columns %}
where purchases.updated_at > (
    select coalesce(max(updated_at), '1900-01-01'::timestamptz)
        - interval '{{ var("incremental_lookback_days", 3) }} days'
    from {{ this }}
)
    {%- endif %}
{% endif %}
