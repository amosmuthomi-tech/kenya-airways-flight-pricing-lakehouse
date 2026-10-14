-- Business question: how does average fare on each route change as
-- the booking window narrows (days_left)? Supports pricing/revenue-
-- management decisions about when fares tend to rise before departure.
-- Grain: one row per (route_key, booking_window_bucket, cabin_class).
select
    route_key,
    source_city,
    destination_city,
    cabin_class,
    booking_window_bucket,
    count(*)                                                          as quote_count,
    round(avg(price_inr), 2)                                          as avg_price_inr,
    min(price_inr)                                                    as min_price_inr,
    max(price_inr)                                                    as max_price_inr
from {{ ref('fct_flight_price_quotes') }}
group by 1, 2, 3, 4, 5
order by route_key, cabin_class,
    case booking_window_bucket
        when '0-7_days' then 1
        when '8-14_days' then 2
        when '15-30_days' then 3
        else 4
    end
