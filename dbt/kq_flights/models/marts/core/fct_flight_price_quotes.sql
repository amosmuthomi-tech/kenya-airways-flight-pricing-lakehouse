-- Grain: one row per fare quote per snapshot -- i.e. "this airline
-- quoted this flight, this route, this time-of-day pattern, this cabin
-- class, at this many days before departure, as of this snapshot."
-- Natural key: (source_row_index, snapshot_date). Two rows sharing the
-- same underlying (airline, flight_code, route, days_left) at a
-- different snapshot_date are a legitimate price observation over
-- time, not a duplicate -- see documentation/incremental_strategy.md #2.
select
    source_row_index::varchar || '|' || snapshot_date::varchar        as quote_id,
    source_row_index,
    snapshot_date,
    airline                                                           as airline_code,
    flight_code,
    route_key,
    source_city,
    destination_city,
    departure_time,
    arrival_time,
    stops,
    cabin_class,
    duration_hours,
    days_left,
    booking_window_bucket,
    price_inr,
    price_per_hour_inr,
    bronze_batch_id
from {{ ref('int_flight_price_quotes_enriched') }}
