# Incremental Processing Strategy

Assumption in force here: each daily file is one `snapshot_date` price-scrape
(see `assumptions.md` #1). This document walks through each of the five
scenarios the brief lists in section 8.

## 1. New records

A new day's file arrives → Bronze ingests it into a new `_snapshot_date`
partition, Silver validates it independently of every other day, and the
fact-table MERGE (see `redshift/README.md`) inserts rows whose
`(source_row_index, snapshot_date)` key doesn't already exist. No historical
partition is touched. This is why `_snapshot_date` is both the Bronze/Silver
partition key and the Redshift sort key — the whole pipeline is designed around
"a day is the unit of work."

## 2. Changed records

Within this dataset's grain, a "changed record" means the same
`(airline, flight, route, time-of-day, class, days_left)` combination is quoted
at a different price than a previous file. Two cases:

- **Same `snapshot_date`, file re-sent with corrections** → handled by
  Bronze's dynamic partition overwrite (rerun-safe, see #4 below) and Silver
  re-validating from scratch — the whole day's partition is simply replaced.
- **Different `snapshot_date`, same underlying flight/route, price moved** →
  this is expected and valid: it's a new row (new `days_left`/`snapshot_date`
  combination), not an update. Price history is the point of keeping every
  snapshot rather than overwriting a "current price" table. The MERGE's
  `WHEN MATCHED THEN UPDATE` branch exists specifically to make same-key
  corrections idempotent without creating a duplicate row.

## 3. Duplicate file delivery

If the same file is delivered twice (same filename/snapshot_date, identical or
near-identical content):

- **Bronze:** dynamic partition overwrite means the second delivery replaces
  the `_snapshot_date` partition rather than appending to it — verified in
  `tests/pyspark/test_bronze_ingest.py`: ingesting the same file twice
  produces the same row count both times.
- **Redshift:** the MERGE keys on `(source_row_index, snapshot_date)`, so
  loading the same day's Silver output twice updates the same rows in place
  instead of inserting a second copy.

## 4. Pipeline reruns

A rerun of any stage (Bronze, Silver, or the dbt build) is safe because every
stage is partition/key-scoped and overwrite-based, not append-only:

- **Bronze:** `partitionOverwriteMode=dynamic` + `.mode("overwrite")` on
  `_snapshot_date`.
- **Silver:** same pattern, re-derived entirely from Bronze for that
  partition — Silver has no state of its own, so replaying it is always safe.
- **dbt:** models are idempotent SELECTs (views/tables fully rebuilt or
  MERGEd from source); running `dbt build` twice in a row produces identical
  output, which is exactly what was verified when building this project (see
  `reproduction_steps.md`).

## 5. Late-arriving data

If a `snapshot_date` file arrives after later dates have already been processed
(e.g. `2026-09-08`'s file shows up after `09-09` and `09-10` are already loaded):

- Because every stage is partitioned/keyed by `_snapshot_date` and not by
  processing order, the late file simply lands in its own correct partition —
  it doesn't need to be inserted "in between" anything or trigger a rebuild of
  the dates around it.
- Downstream Gold marts are grouped by `snapshot_date`, so a late-arriving day
  just adds a new group to the aggregation the next time `dbt build` runs; it
  never changes the aggregated numbers for other days.
- The only thing a late file requires operationally is re-running `dbt build`
  (or `dbt build --select state:modified+` in a real orchestrated setup) so
  the Gold marts pick up the newly available partition — this is a
  scheduling/orchestration concern, not a data-correctness one, given the
  partition-scoped design above.

## Guarantee this gives us for rerun-safety

Because every write in the pipeline is scoped to `_snapshot_date` and is an
overwrite/MERGE of that scope, not a blind INSERT/append, there is no scenario
in the four PySpark + Redshift stages where re-running the same input twice
produces double-counted rows. This was the specific thing tested for Bronze in
this project (see `reproduction_steps.md`).
