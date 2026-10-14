-- Grain: one row per rejected fare quote per snapshot. Keeps every
-- original raw value plus the full list of reasons it was rejected --
-- this feeds the audit mart in models/marts/audit/, which is what a
-- future "the corruption got fixed upstream" reprocess would query.
select
    "index"              as source_row_index,
    airline,
    flight                as raw_flight_code,
    source_city,
    destination_city,
    departure_time,
    arrival_time,
    stops,
    class                  as raw_cabin_class,
    duration_hours,
    days_left,
    price_inr,
    _reject_reasons        as reject_reasons,
    len(_reject_reasons)    as reject_reason_count,
    _snapshot_date::date   as snapshot_date,
    _batch_id              as bronze_batch_id
from read_parquet('{{ var("silver_rejected_path") }}/**/*.parquet', hive_partitioning = 1)
