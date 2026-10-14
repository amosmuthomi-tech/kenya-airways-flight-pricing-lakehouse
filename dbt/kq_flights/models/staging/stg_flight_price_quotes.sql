-- Grain: one row per valid fare quote per snapshot (same grain as
-- Silver's valid output). 1:1 passthrough of Silver, renamed to dbt
-- staging conventions -- no business logic here, just light renaming.
select
    "index"              as source_row_index,
    airline,
    flight                as flight_code,
    source_city,
    destination_city,
    departure_time,
    arrival_time,
    stops,
    class                  as cabin_class,
    duration_hours,
    days_left,
    price_inr,
    _snapshot_date::date   as snapshot_date,
    _batch_id              as bronze_batch_id,
    _row_hash              as bronze_row_hash
from read_parquet('{{ var("silver_valid_path") }}/**/*.parquet', hive_partitioning = 1)
