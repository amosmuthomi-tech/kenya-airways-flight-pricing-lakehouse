-- Grain: unchanged from staging (one row per valid quote per snapshot).
-- Adds derived fields shared by more than one downstream mart, so they
-- aren't recomputed independently in each Gold model.
with stg as (
    select * from {{ ref('stg_flight_price_quotes') }}
)

select
    *,
    source_city || '_' || destination_city                         as route_key,
    round(price_inr / nullif(duration_hours, 0), 2)                 as price_per_hour_inr,
    case
        when days_left <= 7  then '0-7_days'
        when days_left <= 14 then '8-14_days'
        when days_left <= 30 then '15-30_days'
        else '31-365_days'
    end                                                              as booking_window_bucket
from stg
