# Assumptions & Key Design Decisions

This document lists every place the assignment was ambiguous and the assumption
made, per section 12 of the brief ("make a reasonable assumption and document it").

## 1. The dataset has no flight date — only `days_left`

The supplied `airlines_flights_data.csv` is a price-scrape snapshot: every row is
a fare that was observed for a given route/airline/time-of-day combination, at a
certain number of days before departure (`days_left`). There is no calendar date
for the flight itself and no timestamp for when the fare was captured.

**Assumption:** each file represents one day's price-scrape snapshot. We
introduce a `_snapshot_date` field, sourced from the filename convention
`airlines_flights_data_YYYYMMDD.csv` (falling back to the file's ingestion date
if the filename doesn't match). `_snapshot_date` becomes the partition key and
the time dimension for everything downstream — it is what lets section 8 ("a new
pricing file arrives every day") make sense for this dataset.

This also means: the same flight number legitimately repeats thousands of times
in the file (e.g. UK-706 appears 3,235 times) because it's quoted across many
source/destination/time-of-day/class combinations, not because of duplication.
Row-level uniqueness is not the flight number — see #2.

## 2. Natural key / grain

There is no booking ID or scrape ID in the source. The natural key for a fare-quote
record is:

```
(airline, flight, source_city, destination_city, departure_time,
 arrival_time, stops, class, days_left, snapshot_date)
```

i.e. "this airline quoted this flight, this route, this time-of-day pattern, this
cabin class, at this many days before departure, as of this snapshot." In the dbt
fact table this is surfaced as a single surrogate key, `quote_id`
(`source_row_index || '|' || snapshot_date`), which is equivalent for this
dataset since `source_row_index` already uniquely identifies a row within a file.
Two rows matching on the natural key with a different price are treated as a
price change (see `incremental_strategy.md`), not a duplicate.

## 3. "Invalid business values" — what we check for

The raw file is unusually clean (no nulls, no exact duplicate rows). To
demonstrate the DQ approach the brief explicitly asks for (section 9), Silver
validates against a realistic rule set this domain would need in production, and
flags/rejects any row that would violate it, even though the current file happens
to pass almost every rule:

- `price > 0`, `duration > 0`, `days_left` between 0 and 365
- `source_city != destination_city`
- enums (`airline`, `class`, `stops`, `departure_time`, `arrival_time`,
  `source_city`, `destination_city`) restricted to a controlled reference list
- flight-code format sanity check (carrier-code prefix pattern)

## 4. Grain of the dbt fact table

`fct_flight_price_quotes`, grain = one row per fare quote per snapshot (same
tuple as #2). This is documented in the model's SQL header and in
`dbt/kq_flights/models/marts/core/schema.yml`.

## 5. Redshift access

Per section 7 of the brief, no live Redshift cluster is required — DDL and design
rationale only. DDL is written for the Redshift dialect, using DISTKEY/SORTKEY.

## 6. Submission naming

Submitted by Amos Muthomi as `Junior_Data_Engineer_Amos_Muthomi_20260910`.
