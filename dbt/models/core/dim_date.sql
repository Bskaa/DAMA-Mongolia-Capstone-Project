with source_dates as (
    select started_at::date as date_day from {{ ref('stg_sessions') }}
    union all
    select purchase_date::date from {{ ref('stg_purchases') }}
    union all
    select review_date::date from {{ ref('stg_reviews') }}
    union all
    select achieved_at::date from {{ ref('stg_achievements') }}
    union all
    select start_date from {{ ref('stg_marketing_campaigns') }}
    union all
    select end_date from {{ ref('stg_marketing_campaigns') }}
),
bounds as (
    select min(date_day) as min_date, max(date_day) as max_date
    from source_dates
),
calendar as (
    select generate_series(
        min_date::timestamp,
        max_date::timestamp,
        interval '1 day'
    )::date as date_day
    from bounds
)
select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day,
    extract(year from date_day)::integer as year,
    extract(quarter from date_day)::integer as quarter,
    extract(month from date_day)::integer as month,
    to_char(date_day, 'Month') as month_name,
    extract(day from date_day)::integer as day_of_month,
    extract(isodow from date_day)::integer as day_of_week,
    to_char(date_day, 'Day') as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend
from calendar
