"""
Silver layer transformation job.

Reads Bronze Parquet, standardizes types/enums, runs a rule set against
every row, and splits the result into a `valid` and a `rejected` output
-- each rejected row keeps every rule it failed, not just a single
boolean flag, per the brief's "understand why a record was rejected"
(section 9).

See documentation/data_quality_report.md for the full rule catalogue
and what was actually found in this dataset (the Indigo flight-code
corruption).

Usage:
    python3 silver_transform.py \
        --bronze-path ../data/bronze/flight_price_quotes \
        --silver-valid-path ../data/silver/valid \
        --silver-rejected-path ../data/silver/rejected
"""
import argparse

from pyspark.sql import SparkSession, functions as F

CONTROLLED_AIRLINES = ["Indigo", "GO_FIRST", "SpiceJet", "Vistara", "AirAsia", "Air_India"]
CONTROLLED_CLASSES = ["Economy", "Business"]
CONTROLLED_STOPS = ["zero", "one", "two_or_more"]
CONTROLLED_TIMES = ["Early_Morning", "Morning", "Afternoon", "Evening", "Night", "Late_Night"]
CONTROLLED_CITIES = ["Delhi", "Mumbai", "Bangalore", "Kolkata", "Hyderabad", "Chennai"]

# Carrier-code format: 1-3 letters/digits, a dash, 3-5 digits. e.g. 6E-5214, UK-706.
FLIGHT_FORMAT_REGEX = r"^[A-Za-z0-9]{1,3}-\d{3,5}$"
# The specific Excel scientific-notation corruption found in this file:
# a flight code like 6E-5214 gets re-saved as 6.00E-269 by a tool that
# parsed "6E-5214" as a float literal (Excel clamps to a double's real
# exponent range, which is why the exponent bears no relationship to
# the original flight suffix). This pattern is specifically a NEGATIVE
# exponent (e.g. 6.00E-269) -- a value like 0.00E+00 matches the general
# \d\.\d+E[+-]\d+ shape but is NOT this corruption pattern (there's no
# plausible original flight code that collapses to exactly zero), so it
# is deliberately excluded here and falls through to the generic
# flight_code_format reason instead. See documentation/data_quality_report.md.
EXCEL_SCI_NOTATION_REGEX = r"^\d\.\d+E-\d+$"


def add_rule_checks(df):
    """
    Each check appends a named reason string to a `_reject_reasons`
    array when it fails, instead of overwriting a single is_valid flag.
    A row that fails two rules keeps both reasons.

    NOTE: F.array_remove(arr, None) returns NULL (not the filtered
    array) when the input array itself contains a NULL element from a
    `F.when(...).otherwise(F.lit(None))` branch -- an easy trap that
    silently nulls out every row's reasons array. We avoid it entirely
    by only ever appending non-null string literals (via
    F.array(*[...]) built from `F.when(cond, F.array(F.lit(reason))).otherwise(F.array())`
    and concatenating with F.flatten), so there are never NULL elements
    to begin with, and no array_compact/array_remove step is needed.
    """
    def reason_if(condition_is_bad, reason: str):
        return F.when(condition_is_bad, F.array(F.lit(reason))).otherwise(F.array())

    checks = [
        reason_if(~F.col("airline").isin(CONTROLLED_AIRLINES), "airline_not_in_controlled_list"),
        reason_if(~F.col("class").isin(CONTROLLED_CLASSES), "class_not_in_controlled_list"),
        reason_if(~F.col("stops").isin(CONTROLLED_STOPS), "stops_not_in_controlled_list"),
        reason_if(~F.col("departure_time").isin(CONTROLLED_TIMES), "departure_time_not_in_controlled_list"),
        reason_if(~F.col("arrival_time").isin(CONTROLLED_TIMES), "arrival_time_not_in_controlled_list"),
        reason_if(~F.col("source_city").isin(CONTROLLED_CITIES), "source_city_not_in_controlled_list"),
        reason_if(~F.col("destination_city").isin(CONTROLLED_CITIES), "destination_city_not_in_controlled_list"),
        reason_if(F.col("source_city") == F.col("destination_city"), "source_equals_destination"),
        reason_if((F.col("price_inr").isNull()) | (F.col("price_inr") <= 0), "price_not_positive"),
        reason_if((F.col("duration_hours").isNull()) | (F.col("duration_hours") <= 0), "duration_not_positive"),
        reason_if(
            (F.col("days_left").isNull()) | (F.col("days_left") < 0) | (F.col("days_left") > 365),
            "days_left_out_of_range",
        ),
        reason_if(F.col("flight").rlike(EXCEL_SCI_NOTATION_REGEX), "flight_code_excel_scientific_notation_corruption"),
        reason_if(
            (~F.col("flight").rlike(EXCEL_SCI_NOTATION_REGEX)) & (~F.col("flight").rlike(FLIGHT_FORMAT_REGEX)),
            "flight_code_format",
        ),
        reason_if(
            F.col("airline").isNull() | F.col("flight").isNull() | F.col("source_city").isNull()
            | F.col("destination_city").isNull() | F.col("price_inr").isNull(),
            "missing_business_critical_field",
        ),
    ]

    df = df.withColumn("_reject_reasons", F.flatten(F.array(*checks)))
    df = df.withColumn("_is_valid", F.size(F.col("_reject_reasons")) == 0)
    return df


def standardize(df):
    return (
        df.withColumnRenamed("duration", "duration_hours")
        .withColumn("price_inr", F.col("price").cast("long"))
        .withColumn("duration_hours", F.col("duration_hours").cast("double"))
        .withColumn("days_left", F.col("days_left").cast("int"))
        .drop("price")
    )


def run(bronze_path: str, silver_valid_path: str, silver_rejected_path: str, spark=None):
    # Accept an externally-managed SparkSession (used by the test suite,
    # which needs one long-lived session shared across several run()
    # calls) -- only stop the session at the end if this function is
    # the one that created it. SparkContext is a JVM singleton, so
    # calling .stop() on a session created via getOrCreate() would
    # otherwise tear down a session a caller is still using.
    owns_session = spark is None
    if spark is None:
        spark = SparkSession.builder.appName("silver_transform").getOrCreate()

    bronze_df = spark.read.parquet(bronze_path)
    bronze_count = bronze_df.count()

    # Drop exact full-row duplicates, including Bronze's own ingestion
    # metadata -- a true re-ingestion artifact (identical row written
    # twice under two different _batch_id/_row_hash pairs would NOT be
    # caught here on purpose; that case is instead caught by the
    # partition-overwrite idempotency in Bronze). This step catches
    # genuine duplicate source rows.
    deduped_df = bronze_df.dropDuplicates(
        [c for c in bronze_df.columns if not c.startswith("_")]
    )

    standardized_df = standardize(deduped_df)
    checked_df = add_rule_checks(standardized_df)

    valid_df = checked_df.filter(F.col("_is_valid")).drop("_is_valid")
    rejected_df = checked_df.filter(~F.col("_is_valid")).drop("_is_valid")

    valid_count = valid_df.count()
    rejected_count = rejected_df.count()

    (valid_df.write.format("parquet").mode("overwrite")
        .partitionBy("_snapshot_date").save(silver_valid_path))
    (rejected_df.write.format("parquet").mode("overwrite")
        .partitionBy("_snapshot_date").save(silver_rejected_path))

    pct = round(100 * rejected_count / bronze_count, 2) if bronze_count else 0.0
    print(f"total: {bronze_count} valid: {valid_count} rejected: {rejected_count} ({pct}%)")

    reason_counts = (
        rejected_df.withColumn("_reason", F.explode("_reject_reasons"))
        .groupBy("_reason").count().orderBy(F.desc("count"))
        .collect()
    )
    print("[INFO] Rejection reason breakdown:")
    for row in reason_counts:
        print(f"  {row['_reason']:55s} {row['count']}")

    if owns_session:
        spark.stop()
    return {
        "bronze_count": bronze_count,
        "valid_count": valid_count,
        "rejected_count": rejected_count,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-valid-path", required=True)
    parser.add_argument("--silver-rejected-path", required=True)
    args = parser.parse_args()
    run(args.bronze_path, args.silver_valid_path, args.silver_rejected_path)
