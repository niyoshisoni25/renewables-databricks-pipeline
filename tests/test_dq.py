from datetime import datetime
from src.renewables.dq import add_dq_reason, cast_columns, dedupe_latest, split_valid_invalid

RULES = [("not_null", "turbine_id"), ("range", "wind_speed_ms", 0, 40), ("allowed", "status", ["RUNNING", "STOPPED"])]


def make(spark, rows):
    return spark.createDataFrame(rows, "turbine_id string, wind_speed_ms double, status string")


def reasons(spark, rows):
    df = add_dq_reason(make(spark, rows), RULES)
    return [r["_dq_reason"] for r in df.collect()]


def test_good_row_has_empty_reason(spark):
    assert reasons(spark, [("T001", 8.5, "RUNNING")]) == [""]


def test_null_key_is_rejected(spark):
    assert "turbine_id is null" in reasons(spark, [(None, 8.5, "RUNNING")])[0]


def test_range_boundaries_are_inclusive(spark):
    assert reasons(spark, [("T1", 0.0, "RUNNING"), ("T1", 40.0, "RUNNING")]) == ["", ""]
    assert "outside 0-40" in reasons(spark, [("T1", 40.1, "RUNNING")])[0]
    assert "outside 0-40" in reasons(spark, [("T1", -0.1, "RUNNING")])[0]


def test_unknown_status_is_rejected(spark):
    assert "status not in allowed list" in reasons(spark, [("T1", 5.0, "UNKNOWN")])[0]


def test_multiple_failures_are_all_reported(spark):
    r = reasons(spark, [(None, -5.0, "UNKNOWN")])[0]
    assert "turbine_id is null" in r and "wind_speed_ms outside" in r and "status not in allowed list" in r


def test_null_value_in_range_column_is_not_a_range_failure(spark):
    # null handling is the job of not_null rules, so a null wind speed alone should pass the range rule
    assert reasons(spark, [("T1", None, "RUNNING")]) == [""]


def test_split_valid_invalid(spark):
    df = add_dq_reason(make(spark, [("T1", 5.0, "RUNNING"), (None, 5.0, "RUNNING")]), RULES)
    good, bad = split_valid_invalid(df)
    assert good.count() == 1 and bad.count() == 1
    assert "_dq_reason" not in good.columns and "_rejected_at" in bad.columns


def test_bad_cast_becomes_null(spark):
    raw = spark.createDataFrame([("T1", "abc"), ("T2", "7.5")], "turbine_id string, wind_speed_ms string")
    out = {r["turbine_id"]: r["wind_speed_ms"] for r in cast_columns(raw, {"turbine_id": "string", "wind_speed_ms": "double"}).collect()}
    assert out["T1"] is None and out["T2"] == 7.5


def test_dedupe_keeps_newest(spark):
    df = spark.createDataFrame(
        [("R1", 1.0, datetime(2026, 10, 1)), ("R1", 2.0, datetime(2026, 10, 2)), ("R2", 3.0, datetime(2026, 10, 1))],
        "reading_id string, power_kw double, _ingested_at timestamp")
    out = {r["reading_id"]: r["power_kw"] for r in dedupe_latest(df, ["reading_id"]).collect()}
    assert out == {"R1": 2.0, "R2": 3.0}


   def test_null_status_is_rejected_when_not_null_rule_is_added(spark):
       # "allowed" ignores nulls, so a missing status slipped through until we added a not_null rule on it
       assert reasons(spark, [("T1", 8.5, None)]) == [""]
       df = add_dq_reason(make(spark, [("T1", 8.5, None)]), RULES + [("not_null", "status")])
       assert "status is null" in df.collect()[0]["_dq_reason"]
