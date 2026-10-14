-- Grain: one row per airline. Tiny (6 rows) -- see redshift/ddl_core_and_gold.sql
-- for why this is DISTSTYLE ALL in Redshift.
select
    airline                                                          as airline_code,
    case
        when airline in ('Vistara', 'Air_India')       then 'Full Service'
        else 'Low Cost'
    end                                                               as service_tier,
    count(*)                                                          as total_quote_count
from {{ ref('stg_flight_price_quotes') }}
group by 1, 2
