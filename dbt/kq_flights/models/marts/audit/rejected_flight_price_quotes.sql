-- Grain: one row per rejected fare quote per snapshot. Kept
-- indefinitely (not just for the current run) so that a corrected
-- upstream feed or a manual review can be traced back to the exact
-- original raw values and the exact reasons a row didn't make Silver.
-- Queried rarely -- see redshift/README.md for why this is Spectrum
-- rather than a native COPY+MERGE table in production.
select * from {{ ref('stg_rejected_flight_price_quotes') }}
