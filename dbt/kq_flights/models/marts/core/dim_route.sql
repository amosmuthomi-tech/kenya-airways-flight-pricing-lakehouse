-- Grain: one row per source_city/destination_city pair. Small (<=30
-- rows for 6 cities) -- see redshift/ddl_core_and_gold.sql for why this
-- is DISTSTYLE ALL in Redshift.
select
    route_key,
    source_city,
    destination_city,
    count(*)                                                          as total_quote_count
from {{ ref('int_flight_price_quotes_enriched') }}
group by 1, 2, 3
