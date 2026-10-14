# Reproduction Steps & Expected Results

Follow these exactly and you should get the same numbers shown here — this was
run end-to-end while building the project, and again after every code fix, to
confirm nothing was left broken.

## 0. Prerequisites

```bash
python3 --version   # 3.10+ recommended
java -version        # Java 17 or 21 (required by PySpark)
pip install pyspark==3.5.1 dbt-core dbt-duckdb pytest --break-system-packages
```

## 1. Bronze — ingest the raw file

```bash
cd pyspark
python3 bronze/bronze_ingest.py \
  --input ../data/raw/airlines_flights_data_20260908.csv \
  --bronze-path ../data/bronze/flight_price_quotes
```

**Expected:** `source rows (excl. header): 300153, rows parsed by Spark: 300153
(corrupt: 0)`. No row-count mismatch warning.

**Idempotency check** — run the exact same command again, then confirm the
partition still has exactly 300,153 rows (not 600,306):

```bash
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('check').getOrCreate()
print(spark.read.parquet('../data/bronze/flight_price_quotes').count())
"
```

**Expected:** `300153`

## 2. Silver — validate and split

```bash
python3 silver/silver_transform.py \
  --bronze-path ../data/bronze/flight_price_quotes \
  --silver-valid-path ../data/silver/valid \
  --silver-rejected-path ../data/silver/rejected
```

**Expected console output:**

```
total: 300153 valid: 295397 rejected: 4756 (1.58%)
[INFO] Rejection reason breakdown:
  flight_code_excel_scientific_notation_corruption        4705
  flight_code_format                                      51
```

## 3. Run the PySpark test suite

```bash
cd ..
python3 -m pytest tests/pyspark/ -v
```

**Expected:** `9 passed`

## 4. dbt — build and test the model layer

```bash
cd dbt/kq_flights
export DBT_PROFILES_DIR=$(pwd)
dbt build
```

**Expected:** `Done. PASS=40 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=40`
(8 table models + 3 view models + 29 data tests)

Spot-check the fact table row count matches Silver's valid count exactly:

```bash
python3 -c "
import duckdb
con = duckdb.connect('kq_flights_dev.duckdb')
print(con.execute('select count(*) from main_core.fct_flight_price_quotes').fetchone())
"
```

**Expected:** `(295397,)`

## 5. (Optional) inspect a Gold mart

```bash
python3 -c "
import duckdb
con = duckdb.connect('kq_flights_dev.duckdb')
print(con.execute('''
    select * from main_gold.mart_airline_market_share
    where cabin_class = 'Business'
    order by quote_count desc
''').df())
"
```

Business-class quote volume is dominated by Vistara (~65% share) and Air_India
(~35%) — those are the only two carriers offering Business class in this
dataset, which the market-share numbers reflect exactly.

## If your numbers differ

- **Different valid/rejected split** → check you're using the exact supplied
  `airlines_flights_data.csv` (renamed here to include a snapshot-date suffix
  per the filename convention in `assumptions.md`) — the 4,756 rejected count
  is specific to this file's known Excel-corruption pattern, not a general
  threshold.
- **Different dbt test count** → run `dbt clean` then `dbt build` again; a
  stale `target/` from a previous partial run can cause a mismatched count.
- **Bronze idempotency check fails (row count doubles)** → confirm
  `spark.sql.sources.partitionOverwriteMode` is actually being read as
  `dynamic`; it's set in code via `spark.conf.set(...)` in
  `bronze_ingest.py`, which works out of the box with the pip `pyspark`
  package used above, but some Spark distributions require it set in
  `spark-defaults.conf` instead.
