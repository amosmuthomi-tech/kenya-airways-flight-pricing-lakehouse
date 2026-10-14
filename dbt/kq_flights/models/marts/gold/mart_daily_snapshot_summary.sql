-- Business question: for each ingested snapshot, how much data came
-- in, how much of it was usable, and what did the market look like
-- overall? This is the first thing a data-quality/operations
-- stakeholder would check after a new file lands.
-- Grain: one row per snapshot_date.
with valid as (
    select
        snapshot_date,
        count(*)                       as valid_quote_count,
        round(avg(price_inr), 2)       as avg_price_inr
    from {{ ref('fct_flight_price_quotes') }}
    group by 1
),

rejected as (
    select
        snapshot_date,
        count(*)                       as rejected_quote_count
    from {{ ref('stg_rejected_flight_price_quotes') }}
    group by 1
)

select
    coalesce(valid.snapshot_date, rejected.snapshot_date)              as snapshot_date,
    coalesce(valid.valid_quote_count, 0)                               as valid_quote_count,
    coalesce(rejected.rejected_quote_count, 0)                         as rejected_quote_count,
    coalesce(valid.valid_quote_count, 0) + coalesce(rejected.rejected_quote_count, 0)
                                                                        as total_quote_count,
    round(
        100.0 * coalesce(rejected.rejected_quote_count, 0)
        / nullif(coalesce(valid.valid_quote_count, 0) + coalesce(rejected.rejected_quote_count, 0), 0),
        2
    )                                                                   as rejected_pct,
    valid.avg_price_inr
from valid
full outer join rejected using (snapshot_date)
order by snapshot_date
