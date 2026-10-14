# Monitoring & Observability

No monitoring infrastructure was built (per section 10, not required) — this is
the design for production.

## Key metrics to track per run

| Metric | Source | Why |
|---|---|---|
| Source file row count vs. Spark-parsed row count | Bronze log / manifest table | Catches truncated/corrupted file delivery immediately |
| Bronze row count per `_snapshot_date` vs. trailing average | Bronze manifest table | An anomalously low/high row count signals a partial file or a source-side duplication bug |
| Silver valid % vs. rejected % | Silver job stdout / a small metrics table | A sudden jump in rejection rate signals upstream schema drift or a new corruption pattern (like the Excel one found here) |
| Rejection reason distribution | `_reject_reasons` exploded, grouped | Distinguishes "one new weird issue" from "existing known issue got worse" |
| dbt test pass/fail count | `dbt build` exit code + `run_results.json` | Direct pass/fail signal per test, per model |
| Gold mart row counts vs. prior run | dbt / warehouse query | Catches a mart silently going empty (e.g. a broken join) |
| Job duration (Bronze, Silver, `dbt build`) | Orchestrator (e.g. Airflow) task duration | Catches performance regressions before they become SLA misses |
| Redshift load latency / COPY duration | `stl_load_commits`, `svl_query_summary` | Catches slow loads before they cascade into stale dashboards |

## Alert conditions

- **File-level:** source row count vs. parsed row count mismatch → **page**
  (data may be silently wrong downstream).
- **Silver rejection rate:** exceeds a rolling baseline (e.g. >2x the
  trailing 7-day average rejection %) → **alert**, don't page — likely a new
  but non-urgent data-quality issue, needs human triage rather than
  immediate action.
- **A brand-new value in `_reject_reasons`** that wasn't seen in the trailing
  30 days → **alert**. New failure types are more informative than swings in
  the rate of known ones.
- **dbt test failure** on any `not_null`/`unique`/`relationships` test on a
  core table → **page** (breaks referential integrity of the model).
- **Gold mart row count** drops to zero or drops >50% run-over-run → **page**
  (near-certainly a broken join or an empty source, not a real business
  change).
- **Bronze/Silver/dbt job duration** exceeds a rolling p95 by a wide margin
  (e.g. 3x) → **alert** (early warning of a data-volume spike or a
  performance regression, before it becomes a missed SLA).
- **No file arrives by the expected daily cutoff time** → **alert**
  (missing-data case, distinct from a bad-data case).

## What "page" vs. "alert" means here

**Page** = wakes someone up now, used only for things that mean the pipeline
produced (or is about to produce) wrong numbers that a business user could act
on. **Alert** = goes to a channel/ticket for the next business day, used for
things that are interesting but not yet actively harmful (e.g. a
rejection-rate uptick that's still within the Silver layer's designed-for
tolerance).
