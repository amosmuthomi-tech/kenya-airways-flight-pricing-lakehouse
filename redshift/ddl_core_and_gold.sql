-- Redshift DDL for the core dimensional model and Gold marts.
-- See redshift/README.md for the load strategy and the reasoning
-- behind every DISTKEY/SORTKEY choice below, and
-- documentation/incremental_strategy.md for how MERGE keeps this
-- rerun/duplicate-safe.
--
-- No live cluster was used to execute this DDL (not required per
-- section 7 of the brief); dbt was run against DuckDB locally with the
-- identical model SQL to validate the transformation logic itself.

create schema if not exists core;
create schema if not exists gold;
create schema if not exists audit;

-- ============================================================
-- core.dim_airline
-- Grain: one row per airline. 6 rows.
-- DISTSTYLE ALL: tiny enough that broadcasting a full copy to every
-- compute node avoids a shuffle on every join against the fact table.
-- ============================================================
create table if not exists core.dim_airline (
    airline_code        varchar(32)   not null,
    service_tier         varchar(16)   not null,
    total_quote_count    bigint,
    primary key (airline_code)
)
diststyle all
sortkey (airline_code);

-- ============================================================
-- core.dim_route
-- Grain: one row per source_city/destination_city pair. <=30 rows for
-- 6 cities. DISTSTYLE ALL for the same reason as dim_airline.
-- ============================================================
create table if not exists core.dim_route (
    route_key             varchar(64)  not null,
    source_city            varchar(32)  not null,
    destination_city       varchar(32)  not null,
    total_quote_count      bigint,
    primary key (route_key)
)
diststyle all
sortkey (route_key);

-- ============================================================
-- core.fct_flight_price_quotes
-- Grain: one row per fare quote per snapshot. Natural key:
-- (source_row_index, snapshot_date), surfaced as quote_id.
-- DISTKEY(route_key): the only table large enough for distribution
-- style to matter; 2 of the 4 Gold marts aggregate by route, so
-- co-locating fact rows by route minimizes shuffle for that
-- join/aggregation. Because the dimensions are DISTSTYLE ALL, this
-- costs nothing on the dim_airline join.
-- COMPOUND SORTKEY(snapshot_date, airline_code): nearly every query
-- (incremental load, dashboard, mart refresh) filters by a date range
-- first -- a compound sort key on snapshot_date lets zone maps skip
-- entire blocks outside the requested range, which is the single
-- biggest lever for keeping scans cheap as this table grows by
-- ~300K rows/day. airline_code as the second key speeds per-airline
-- aggregation within a date range.
-- ============================================================
create table if not exists core.fct_flight_price_quotes (
    quote_id               varchar(64)   not null,
    source_row_index       int           not null,
    snapshot_date           date          not null,
    airline_code             varchar(32)   not null,
    flight_code               varchar(32)   not null,
    route_key                  varchar(64)   not null,
    source_city                 varchar(32)   not null,
    destination_city             varchar(32)   not null,
    departure_time                 varchar(16)   not null,
    arrival_time                     varchar(16)   not null,
    stops                              varchar(16)   not null,
    cabin_class                          varchar(16)   not null,
    duration_hours                         decimal(6, 2) not null,
    days_left                                int           not null,
    booking_window_bucket                      varchar(16)   not null,
    price_inr                                    bigint        not null,
    price_per_hour_inr                              decimal(10, 2),
    bronze_batch_id                                    varchar(64),
    primary key (quote_id)
)
diststyle key
distkey (route_key)
compound sortkey (snapshot_date, airline_code);

-- ============================================================
-- gold.mart_airline_market_share
-- Grain: one row per (airline_code, cabin_class).
-- gold.mart_route_price_trends
-- Grain: one row per (route_key, booking_window_bucket, cabin_class).
-- gold.mart_daily_snapshot_summary
-- Grain: one row per snapshot_date.
-- gold.mart_cabin_class_performance
-- Grain: one row per (cabin_class, stops).
-- All four are small, pre-aggregated outputs with no dominant join
-- pattern -> DISTSTYLE EVEN avoids skew; SORTKEY(snapshot_date) or the
-- mart's natural grouping keeps the same pruning benefit for BI tools
-- that typically filter by reporting period.
-- ============================================================
create table if not exists gold.mart_airline_market_share (
    airline_code                     varchar(32)   not null,
    cabin_class                        varchar(16)   not null,
    quote_count                          bigint        not null,
    market_share_pct_within_class          decimal(5, 2) not null,
    avg_price_inr                            decimal(10, 2) not null
)
diststyle even
sortkey (cabin_class, airline_code);

create table if not exists gold.mart_route_price_trends (
    route_key                    varchar(64)   not null,
    source_city                    varchar(32)   not null,
    destination_city                  varchar(32)   not null,
    cabin_class                          varchar(16)   not null,
    booking_window_bucket                  varchar(16)   not null,
    quote_count                              bigint        not null,
    avg_price_inr                              decimal(10, 2) not null,
    min_price_inr                                bigint,
    max_price_inr                                  bigint
)
diststyle even
sortkey (route_key, cabin_class);

create table if not exists gold.mart_daily_snapshot_summary (
    snapshot_date              date          not null,
    valid_quote_count            bigint        not null,
    rejected_quote_count           bigint        not null,
    total_quote_count                bigint        not null,
    rejected_pct                       decimal(5, 2),
    avg_price_inr                        decimal(10, 2),
    primary key (snapshot_date)
)
diststyle even
sortkey (snapshot_date);

create table if not exists gold.mart_cabin_class_performance (
    cabin_class                varchar(16)   not null,
    stops                        varchar(16)   not null,
    quote_count                    bigint        not null,
    avg_price_inr                    decimal(10, 2) not null,
    avg_duration_hours                 decimal(6, 2)  not null,
    avg_price_per_hour_inr               decimal(10, 2)
)
diststyle even
sortkey (cabin_class, stops);

-- ============================================================
-- audit.rejected_flight_price_quotes
-- Grain: one row per rejected fare quote per snapshot. Queried rarely
-- (debugging / reconciliation only) -- see redshift/README.md for why
-- this is exposed via Spectrum rather than a native COPY+MERGE table.
-- ============================================================
create table if not exists audit.rejected_flight_price_quotes (
    source_row_index          int           not null,
    airline                     varchar(32),
    raw_flight_code               varchar(64),
    source_city                     varchar(32),
    destination_city                   varchar(32),
    departure_time                       varchar(16),
    arrival_time                           varchar(16),
    stops                                    varchar(16),
    raw_cabin_class                            varchar(16),
    duration_hours                               decimal(6, 2),
    days_left                                      int,
    price_inr                                        bigint,
    reject_reasons                                     super,
    reject_reason_count                                  int,
    snapshot_date                                          date,
    bronze_batch_id                                          varchar(64)
)
diststyle even
sortkey (snapshot_date);
