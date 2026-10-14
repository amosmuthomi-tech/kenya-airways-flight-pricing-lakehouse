import csv
import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pyspark", "bronze"))
import bronze_ingest  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.appName("test_bronze").master("local[2]").getOrCreate()
    yield s
    s.stop()


@pytest.fixture()
def small_csv(tmp_path):
    path = tmp_path / "airlines_flights_data_20260910.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "index", "airline", "flight", "source_city", "departure_time", "stops",
            "arrival_time", "destination_city", "class", "duration", "days_left", "price",
        ])
        w.writerow([0, "SpiceJet", "SG-8709", "Delhi", "Evening", "zero", "Night", "Mumbai", "Economy", 2.17, 1, 5953])
        w.writerow([1, "Indigo", "6.00E-269", "Delhi", "Morning", "one", "Evening", "Mumbai", "Economy", 5.5, 2, 4500])
    return str(path)


def test_derive_snapshot_date_from_filename():
    assert bronze_ingest.derive_snapshot_date("airlines_flights_data_20260908.csv") == "2026-09-08"


def test_derive_snapshot_date_fallback_when_no_date_in_filename():
    result = bronze_ingest.derive_snapshot_date("airlines_flights_data.csv")
    # Falls back to today's date (ingestion date) -- just check it's a valid ISO date string.
    assert len(result) == 10 and result.count("-") == 2


def test_row_count_excl_header(small_csv):
    assert bronze_ingest.row_count_excl_header(small_csv) == 2


def test_bronze_ingest_adds_metadata_columns(spark, small_csv, tmp_path):
    bronze_path = str(tmp_path / "bronze_out")
    manifest_path = str(tmp_path / "manifest_out")
    bronze_ingest.run(small_csv, bronze_path, manifest_path)
    # re-open a fresh session read (run() stops its own session; use the module-scoped one to read output)
    s2 = SparkSession.builder.appName("read_check").master("local[2]").getOrCreate()
    df = s2.read.parquet(bronze_path)
    expected_meta_cols = {"_source_file_name", "_ingestion_ts", "_snapshot_date", "_batch_id", "_row_hash"}
    assert expected_meta_cols.issubset(set(df.columns))
    assert df.count() == 2


def test_bronze_ingest_is_idempotent_on_rerun(small_csv, tmp_path):
    bronze_path = str(tmp_path / "bronze_out2")
    manifest_path = str(tmp_path / "manifest_out2")
    bronze_ingest.run(small_csv, bronze_path, manifest_path)
    bronze_ingest.run(small_csv, bronze_path, manifest_path)
    s2 = SparkSession.builder.appName("read_check2").master("local[2]").getOrCreate()
    count = s2.read.parquet(bronze_path).count()
    assert count == 2, "re-running against the same file must not double the row count"


def test_bronze_ingest_no_row_count_mismatch(small_csv, tmp_path, capsys):
    bronze_path = str(tmp_path / "bronze_out3")
    manifest_path = str(tmp_path / "manifest_out3")
    result = bronze_ingest.run(small_csv, bronze_path, manifest_path)
    assert result["source_row_count"] == result["parsed_row_count"] == 2
    assert result["corrupt_row_count"] == 0
