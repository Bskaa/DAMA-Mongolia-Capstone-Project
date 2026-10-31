-- Calendar from the earliest activity date to one year after today, so that
-- incremental facts never reference a missing date.
with source_dates as (
    select {{ reporting_date('started_at') }} as date_day from {{ ref('stg_sessions') }}
    union all
    select {{ reporting_date('purchase_date') }} from {{ ref('stg_purchases') }}
    union all
    select {{ reporting_date('review_date') }} from {{ ref('stg_reviews') }}
    union all
    select {{ reporting_date('achieved_at') }} from {{ ref('stg_achievements') }}
    union all
    select start_date from {{ ref('stg_marketing_campaigns') }}
    union all
    select end_date from {{ ref('stg_marketing_campaigns') }}
),
bounds as (
    select
        coalesce(min(date_day), current_date) as min_date,
        greatest(coalesce(max(date_day), current_date), current_date) + 365 as max_date
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
    {{ date_key('date_day') }} as date_key,
    date_day,
    extract(year from date_day)::integer as year,
    extract(quarter from date_day)::integer as quarter,
    extract(month from date_day)::integer as month,
    trim(to_char(date_day, 'Month')) as month_name,
    date_trunc('month', date_day)::date as month_start,
    extract(day from date_day)::integer as day_of_month,
    extract(isodow from date_day)::integer as day_of_week,
    trim(to_char(date_day, 'Day')) as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend
from calendar
