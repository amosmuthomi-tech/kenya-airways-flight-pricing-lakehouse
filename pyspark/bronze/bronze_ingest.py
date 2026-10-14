"""
Bronze layer ingestion job.

Reads a raw airline fare-quote CSV file, attaches source + ingestion
metadata, and writes it to a Parquet Bronze table partitioned by
_snapshot_date. Rerun-safe: uses dynamic partition overwrite, so
re-running the job against the same file replaces that partition
instead of duplicating rows.

See documentation/assumptions.md (#1, #2) for why _snapshot_date exists
and how it's derived, and documentation/incremental_strategy.md (#3, #4)
for the duplicate-delivery / rerun guarantees this design gives us.

Usage:
    python3 bronze_ingest.py \
        --input ../data/raw/airlines_flights_data_20260908.csv \
        --bronze-path ../data/bronze/flight_price_quotes
"""
import argparse
import hashlib
import os
import re
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, types as T

# Explicit schema. We do NOT use inferSchema=True: an inferred schema
# silently changes if a future file's column order, types, or presence
# changes, which defeats the "unexpected schema changes" DQ control in
# section 9 of the brief. With an explicit schema, a missing/renamed
# required column fails loudly instead of silently reshaping the table.
RAW_SCHEMA = T.StructType([
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
])

FILENAME_DATE_RE = re.compile(r"(\d{8})")


def derive_snapshot_date(input_path: str) -> str:
    """
    Assumption #1 (documentation/assumptions.md): the dataset carries no
    flight date, only days_left. We treat each file drop as one day's
    price-scrape snapshot, keyed by a date parsed from the filename
    (airlines_flights_data_YYYYMMDD.csv). If the filename doesn't match
    that convention, fall back to today's date (the ingestion date) so
    the job never fails outright on a missing filename convention --
    it degrades to "this file is today's snapshot" instead.
    """
    base = os.path.basename(input_path)
    m = FILENAME_DATE_RE.search(base)
    if m:
        raw = m.group(1)
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def row_count_excl_header(path: str) -> int:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f) - 1


def write_manifest_row(manifest_path: str, spark: SparkSession, record: dict):
    """
    Append-only manifest table: one row per Bronze run. This is the seed
    of the lineage/monitoring story in documentation/monitoring.md --
    "Source file row count vs. Spark-parsed row count" is read straight
    off this table.
    """
    schema = T.StructType([
        T.StructField("batch_id", T.StringType(), False),
        T.StructField("source_file_name", T.StringType(), False),
        T.StructField("snapshot_date", T.StringType(), False),
        T.StructField("source_row_count", T.LongType(), False),
        T.StructField("parsed_row_count", T.LongType(), False),
        T.StructField("corrupt_row_count", T.LongType(), False),
        T.StructField("run_ts", T.TimestampType(), False),
    ])
    row = [(
        record["batch_id"], record["source_file_name"], record["snapshot_date"],
        record["source_row_count"], record["parsed_row_count"],
        record["corrupt_row_count"], record["run_ts"],
    )]
    df = spark.createDataFrame(row, schema=schema)
    (df.write.format("parquet").mode("append").save(manifest_path))


def run(input_path: str, bronze_path: str, manifest_path: str) -> dict:
    spark = (
        SparkSession.builder.appName("bronze_ingest")
        # Dynamic partition overwrite: writing to _snapshot_date=X only
        # replaces partition X, never the whole table. This is what
        # makes reruns and duplicate-file-delivery safe (see
        # documentation/incremental_strategy.md #3, #4).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    snapshot_date = derive_snapshot_date(input_path)
    batch_id = str(uuid.uuid4())
    source_file_name = os.path.basename(input_path)
    ingestion_ts = datetime.now(timezone.utc)

    # NOTE: StructType.add() mutates and returns self in PySpark, so we
    # build a fresh StructType from RAW_SCHEMA's fields rather than
    # calling RAW_SCHEMA.add(...) directly -- that would permanently
    # (and cumulatively, on every call) append _corrupt_record to the
    # shared module-level RAW_SCHEMA object.
    read_schema = T.StructType(list(RAW_SCHEMA.fields) + [
        T.StructField("_corrupt_record", T.StringType(), True)
    ])
    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(read_schema)
        .csv(input_path)
    )

    parsed_count = df.count()
    corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
    source_count = row_count_excl_header(input_path)

    if source_count != parsed_count:
        print(
            f"[WARN] source/parsed row-count mismatch: "
            f"source={source_count} parsed={parsed_count}. "
            f"See documentation/monitoring.md (file-level alert).",
            file=sys.stderr,
        )
    else:
        print(f"source rows (excl. header): {source_count}, "
              f"rows parsed by Spark: {parsed_count} (corrupt: {corrupt_count})")

    row_hash_cols = [c.name for c in RAW_SCHEMA.fields]
    bronze_df = (
        df.withColumn("_source_file_name", F.lit(source_file_name))
        .withColumn("_ingestion_ts", F.lit(ingestion_ts))
        .withColumn("_snapshot_date", F.lit(snapshot_date))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn(
            "_row_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_hash_cols]), 256),
        )
    )

    (
        bronze_df.write.format("parquet")
        .mode("overwrite")
        .partitionBy("_snapshot_date")
        .save(bronze_path)
    )

    write_manifest_row(manifest_path, spark, {
        "batch_id": batch_id,
        "source_file_name": source_file_name,
        "snapshot_date": snapshot_date,
        "source_row_count": source_count,
        "parsed_row_count": parsed_count,
        "corrupt_row_count": corrupt_count,
        "run_ts": ingestion_ts,
    })

    result = {
        "batch_id": batch_id,
        "snapshot_date": snapshot_date,
        "source_row_count": source_count,
        "parsed_row_count": parsed_count,
        "corrupt_row_count": corrupt_count,
    }
    spark.stop()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--manifest-path", default=None)
    args = parser.parse_args()

    manifest_path = args.manifest_path or os.path.join(
        os.path.dirname(args.bronze_path), "_manifest"
    )
    run(args.input, args.bronze_path, manifest_path)
