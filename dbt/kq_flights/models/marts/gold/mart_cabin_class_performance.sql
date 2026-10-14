-- Business question: how do price and duration vary by cabin class and
-- number of stops? Supports understanding the direct-vs-connecting and
-- Economy-vs-Business price premium, which is a natural input to fare
-- strategy and network planning.
-- Grain: one row per (cabin_class, stops).
select
    cabin_class,
    stops,
    count(*)                                                          as quote_count,
    round(avg(price_inr), 2)                                          as avg_price_inr,
    round(avg(duration_hours), 2)                                     as avg_duration_hours,
    round(avg(price_per_hour_inr), 2)                                 as avg_price_per_hour_inr
from {{ ref('fct_flight_price_quotes') }}
group by 1, 2
order by cabin_class,
    case stops when 'zero' then 1 when 'one' then 2 else 3 end
