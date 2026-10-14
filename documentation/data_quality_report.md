# Data Quality & Reconciliation

## What was actually found in this dataset (not hypothetical)

Profiling `airlines_flights_data.csv` (300,153 rows) found:

| Check | Result |
|---|---|
| Nulls in any column | 0 |
| Exact full-row duplicates | 0 |
| Rows where `source_city == destination_city` | 0 |
| Rows failing the flight-code format rule | 4,756 (1.58%) |

The 4,756 failures are not random noise — they cluster entirely on the Indigo
(`6E`) carrier and follow the exact shape of an Excel scientific-notation
corruption: a flight code like `6E-5214` was at some point re-opened and
re-saved in a tool (almost certainly Excel) that interpreted `6E-5214` as the
number 6 × 10⁻⁵²¹⁴ and wrote it back out as `6.00E-269` (Excel clamps to a
double's real exponent range, which is why the exponent in the corrupted value
bears no relationship to the original flight suffix). Of the 4,756:

- **4,705** match the exact `\d\.\d+E-\d+` corruption pattern and are tagged
  `flight_code_excel_scientific_notation_corruption`.
- **51** are a different corrupted value (`0.00E+00`) and fail the format rule
  for a distinct reason, tagged generically as `flight_code_format` so the two
  populations don't get conflated in monitoring.

**Decision: reject, don't guess-fix.** Because the exponent doesn't encode the
original suffix, there's no reliable way to recover the true flight number —
attempting to "fix" it (e.g. always mapping to `6E-0000`) would silently
fabricate flight numbers. Rejecting with a clear, specific reason is the more
honest engineering choice, and it's fully reversible:
`audit.rejected_flight_price_quotes` (Redshift) / `silver/rejected` (local)
keeps every rejected row with its original raw flight value, so if a future
source system fixes the corruption upstream, or a re-scrape supersedes these
rows, nothing is lost.

## Validation rules implemented (Silver layer)

Implemented in `pyspark/silver/silver_transform.py::add_rule_checks`:

| Rule | Catches |
|---|---|
| `airline` in controlled list | unexpected/misspelled carrier |
| `class` in {Economy, Business} | schema/enum drift |
| `stops` in {zero, one, two_or_more} | schema/enum drift |
| `departure_time` / `arrival_time` in controlled list | schema/enum drift |
| `source_city` / `destination_city` in controlled list | unexpected route data |
| `source_city != destination_city` | invalid business value (self-loop route) |
| `price > 0` | invalid/missing price |
| `duration > 0` | invalid/missing duration |
| `days_left` between 0 and 365 | invalid business value (booking window) |
| flight matches carrier-code format | malformed identifiers (see corruption above) |
| business-critical fields not null | missing values |

Every rule that fails is appended to a `_reject_reasons` array rather than using
a single boolean `is_valid` flag with no explanation — a record that fails two
rules keeps both reasons, which is what the brief asks for ("understand why a
record was rejected").

## Duplicate handling

`silver_transform.py` drops exact full-row duplicates (`.dropDuplicates()` on
the non-metadata columns) before validation runs, so a true source-level
duplicate is caught here on top of Bronze's own partition-overwrite
idempotency (see `incremental_strategy.md` #3–4). No exact duplicates exist in
the current file; the check is there because a production feed is never
guaranteed to stay that clean.

## Unexpected schema changes

Handled at the earliest possible point — Bronze's explicit `RAW_SCHEMA` plus
`PERMISSIVE` mode with `columnNameOfCorruptRecord`. If a future file adds,
removes, renames, or retypes a column, PySpark either raises (missing required
column) or routes the offending rows into `_corrupt_record` instead of silently
coercing bad data — Bronze's `[WARN]` log line for a source/parsed row-count
mismatch is the trigger a monitoring alert would key off (see `monitoring.md`).

## Source-to-target reconciliation

Reconciliation is checked at every hop, not just once at the end:

1. **Raw file → Bronze:** line count in the raw file vs. rows Spark parsed
   (logged every run; mismatch triggers a `[WARN]`).
2. **Bronze → Silver:** `valid_count + rejected_count == bronze_count` is an
   arithmetic identity of how Silver is built (every Bronze row lands in
   exactly one of the two outputs), verified in
   `tests/pyspark/test_silver_transform.py`.
3. **Silver → dbt fact table:** `fct_flight_price_quotes` row count checked
   against Silver's `valid` row count in `reproduction_steps.md`
   (**295,397** in both, confirmed while building this project).
