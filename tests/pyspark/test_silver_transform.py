import os
import sys

import pytest
from pyspark.sql import SparkSession, types as T

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pyspark", "silver"))
import silver_transform  # noqa: E402

BRONZE_SCHEMA = T.StructType([
    T.StructField("index", T.IntegerType(), True),
    T.StructField("airline", T.StringType(), True),
    T.StructField("flight", T.StringType(), True),
    T.StructField("source_city", T.StringType(), True),
    T.StructField("departure_time", T.StringType(), True),
    T.StructField("stops", T.StringType(), True),
    T.StructField("arrival_time", T.StringType(), True),
    T.StructField("destination_city", T.StringType(), True),
    T.StructField("class", T.StringType(), True),
    T.StructField("duration", T.DoubleType(), True),
    T.StructField("days_left", T.IntegerType(), True),
    T.StructField("price", T.LongType(), True),
    T.StructField("_corrupt_record", T.StringType(), True),
    T.StructField("_source_file_name", T.StringType(), True),
    T.StructField("_ingestion_ts", T.TimestampType(), True),
    T.StructField("_batch_id", T.StringType(), True),
    T.StructField("_row_hash", T.StringType(), True),
    T.StructField("_snapshot_date", T.StringType(), True),
])


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.appName("test_silver").master("local[2]").getOrCreate()
    yield s
    s.stop()


def make_bronze_df(spark, rows):
    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)


def test_valid_rejected_counts_sum_to_bronze_count(spark, tmp_path):
    rows = [
        (0, "SpiceJet", "SG-8709", "Delhi", "Evening", "zero", "Night", "Mumbai", "Economy", 2.17, 1, 5953,
         None, "f.csv", None, "b1", "h1", "2026-09-08"),
        (1, "Indigo", "6.00E-269", "Delhi", "Morning", "one", "Evening", "Mumbai", "Economy", 5.5, 2, 4500,
         None, "f.csv", None, "b1", "h2", "2026-09-08"),
        (2, "SpiceJet", "SG-1234", "Delhi", "Morning", "zero", "Evening", "Delhi", "Economy", 3.0, 5, 3000,
         None, "f.csv", None, "b1", "h3", "2026-09-08"),  # source_city == destination_city -> reject
    ]
    df = make_bronze_df(spark, rows)
    bronze_path = str(tmp_path / "bronze")
    df.write.parquet(bronze_path)

    valid_path, rejected_path = str(tmp_path / "valid"), str(tmp_path / "rejected")
    result = silver_transform.run(bronze_path, valid_path, rejected_path, spark=spark)

    assert result["valid_count"] + result["rejected_count"] == result["bronze_count"]


def test_excel_scientific_notation_flight_code_is_rejected_with_specific_reason(spark, tmp_path):
    rows = [
        (0, "Indigo", "6.00E-269", "Delhi", "Morning", "one", "Evening", "Mumbai", "Economy", 5.5, 2, 4500,
         None, "f.csv", None, "b1", "h1", "2026-09-08"),
    ]
    df = make_bronze_df(spark, rows)
    checked = silver_transform.add_rule_checks(silver_transform.standardize(df))
    reasons = checked.select("_reject_reasons").first()["_reject_reasons"]
    assert "flight_code_excel_scientific_notation_corruption" in reasons


def test_exact_duplicate_rows_are_dropped(spark, tmp_path):
    row = (0, "SpiceJet", "SG-8709", "Delhi", "Evening", "zero", "Night", "Mumbai", "Economy", 2.17, 1, 5953,
           None, "f.csv", None, "b1", "h1", "2026-09-08")
    df = make_bronze_df(spark, [row, row])  # exact duplicate
    bronze_path = str(tmp_path / "bronze_dupe")
    df.write.parquet(bronze_path)
    valid_path, rejected_path = str(tmp_path / "valid_dupe"), str(tmp_path / "rejected_dupe")
    result = silver_transform.run(bronze_path, valid_path, rejected_path, spark=spark)
    assert result["bronze_count"] == 2
    assert result["valid_count"] + result["rejected_count"] == 1
