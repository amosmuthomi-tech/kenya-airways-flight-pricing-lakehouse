-- Business question: which airlines carry the most fare-quote volume,
-- overall and within each cabin class? Useful for competitive
-- positioning and for sanity-checking whether a given carrier's
-- inventory/pricing coverage looks right in a given snapshot window.
-- Grain: one row per (airline_code, cabin_class).
with fct as (
    select * from {{ ref('fct_flight_price_quotes') }}
),

totals as (
    select cabin_class, count(*) as class_total_quotes
    from fct
    group by 1
)

select
    fct.airline_code,
    fct.cabin_class,
    count(*)                                                          as quote_count,
    round(100.0 * count(*) / totals.class_total_quotes, 2)            as market_share_pct_within_class,
    round(avg(fct.price_inr), 2)                                       as avg_price_inr
from fct
join totals using (cabin_class)
group by 1, 2, totals.class_total_quotes
order by cabin_class, quote_count desc
