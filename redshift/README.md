# Redshift Design & Load Strategy

No live Redshift cluster was used for this exercise (not provided or required per
section 7 of the brief). This document covers the design decisions behind
`ddl_core_and_gold.sql` and how the data would actually get loaded and kept
incrementally fresh in production.

## How data lands in Redshift

Silver's Parquet output (from the PySpark job) lands in S3, partitioned by
`_snapshot_date`. Two loading options are compatible with the DDL as written:

1. **Redshift Spectrum** — register the S3 Silver location as an external
   schema/table and query it directly, or `CREATE TABLE AS SELECT` from the
   external table into the native core/gold tables. Zero-copy, cheapest to
   keep fresh, best when Silver's partitioning already matches how Redshift
   will scan it.
2. **COPY into a staging table, then MERGE** into the target native table.
   Better performance for tables queried very frequently, since native
   Redshift storage plus our sort keys give tighter zone-map pruning than
   Spectrum's file-level pruning.

This design uses COPY + staging + MERGE for `core.fct_flight_price_quotes`
(the highest-traffic table) and Spectrum for ad-hoc access to
`audit.rejected_flight_price_quotes` (queried rarely, mostly for debugging).

## Load pattern for the fact table (idempotent, incremental)

```sql
-- 1. Load the day's new Silver output into a staging table.
create temp table stg_fct_flight_price_quotes (like core.fct_flight_price_quotes);

copy stg_fct_flight_price_quotes
from 's3://<bucket>/silver/valid/_snapshot_date=2026-09-08/'
iam_role 'arn:aws:iam::<account>:role/redshift-copy-role'
format as parquet;

-- 2. MERGE on the natural key (source_row_index, snapshot_date). This is
--    what makes reruns and duplicate file delivery safe: re-running the
--    same day's load simply re-matches and replaces the same rows
--    instead of duplicating them.
merge into core.fct_flight_price_quotes as tgt
using stg_fct_flight_price_quotes as src
on tgt.source_row_index = src.source_row_index
and tgt.snapshot_date = src.snapshot_date
when matched then update set
    price_inr = src.price_inr,
    route_key = src.route_key,
    duration_hours = src.duration_hours,
    price_per_hour_inr = src.price_per_hour_inr
when not matched then insert values (src.*);
```

See `documentation/incremental_strategy.md` for how this handles new records,
changed prices, duplicate files, reruns, and late-arriving data end to end —
not just the Redshift piece.

## Why these DISTKEY / SORTKEY choices

- **`dim_airline` / `dim_route` → `DISTSTYLE ALL`**: both are tiny (6 and
  ≤36 rows). Broadcasting a full copy to every compute node means the join
  against the much larger fact table never needs a network shuffle.
- **`fct_flight_price_quotes` → `DISTKEY(route_key)`**: the fact table is the
  only one large enough for distribution style to matter. Route-level
  aggregation is the heaviest recurring workload (2 of the 4 Gold marts
  group by route), so co-locating fact rows by route minimizes shuffle for
  that join/aggregation. Because the dimensions are `DISTSTYLE ALL`, this
  costs nothing extra on the `dim_airline` join.
- **`COMPOUND SORTKEY(snapshot_date, airline_code)`**: nearly every query
  (incremental load, dashboard, mart refresh) filters by a date range first.
  A compound sort key on `snapshot_date` lets Redshift's zone maps skip
  entire blocks that fall outside the requested range — the single biggest
  lever for keeping scans cheap as the table grows. `airline_code` as the
  second key speeds per-airline aggregation within a date range.
- **Gold marts → `DISTSTYLE EVEN` + `SORTKEY(snapshot_date, ...)`**: small,
  pre-aggregated outputs with no dominant join pattern, so even distribution
  avoids skew; sorting keeps the same pruning benefit for BI tools that
  typically filter by reporting period.

## How this design behaves as volume grows significantly

The current file is a one-time 300K-row snapshot; in production this table
grows by roughly that many rows per day.

- **Short-to-medium term** (data comfortably fits the cluster): the
  compound sort key on `snapshot_date` keeps scans cheap because BI/mart
  queries almost always filter to a recent date range — zone maps let
  Redshift skip the vast majority of blocks for older dates. Routine
  `VACUUM`/`ANALYZE` (or `auto-analyze`/auto-vacuum) keep sort order and
  statistics correct as MERGE-driven updates accumulate unsorted regions.
- **DISTKEY skew risk**: `route_key` distribution assumes route volume is
  reasonably even across ~30 routes, which holds for this dataset. If the
  network expands to include a few very high-volume hub routes, that key
  could start to skew slices unevenly — worth re-evaluating with
  `svv_table_info` skew metrics, and possibly moving to `DISTSTYLE EVEN` or
  a hashed compound key at that point.
- **Long term** (multi-year history, cluster storage becomes expensive):
  cold partitions (e.g. anything older than a rolling 12–18 month window)
  would be unloaded to S3 (`UNLOAD`) and re-registered as a Spectrum
  external table, so old data stays queryable (unioned with the native
  table via a view) without paying for native cluster storage/compute on
  data that's rarely queried — the natural next step of the same
  Bronze/Silver/Gold-in-S3 architecture already used upstream.
- **Compression**: encodings aren't hand-specified in the DDL; running
  `ANALYZE COMPRESSION` after the first real load (or relying on
  `ENCODE AUTO`, Redshift's default since RA3 nodes) lets Redshift pick
  optimal per-column encodings from actual data rather than a guess made
  before any data exists.
