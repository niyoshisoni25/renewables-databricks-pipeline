"""Reusable data quality helpers (kept out of the notebook so they can be unit tested)."""
from pyspark.sql import DataFrame, Window, functions as F


def rule_failure(rule):
    """Turn one rule tuple into a Spark column that holds the failure message, or null if the row passes.

    Supported rules: ("not_null", col), ("range", col, low, high), ("allowed", col, [values])
    """
    kind, col = rule[0], rule[1]
    c = F.col(col)
    if kind == "not_null":
        return F.when(c.isNull(), F.lit(f"{col} is null"))
    if kind == "range":
        return F.when(c.isNotNull() & ((c < rule[2]) | (c > rule[3])), F.lit(f"{col} outside {rule[2]}-{rule[3]}"))
    if kind == "allowed":
        return F.when(c.isNotNull() & ~c.isin(rule[2]), F.lit(f"{col} not in allowed list"))
    raise ValueError(f"Unknown rule type: {kind}")


def cast_columns(df: DataFrame, columns: dict, keep=()) -> DataFrame:
    """Cast text columns to their configured types. Values that cannot be cast become null."""
    # try_cast returns null instead of raising an error, which matters on Databricks where ANSI mode is on by default
    return df.select(*[F.expr(f"try_cast(`{c}` AS {t})").alias(c) for c, t in columns.items()], *keep)


def add_dq_reason(df: DataFrame, rules) -> DataFrame:
    """Add _dq_reason: empty string when the row passes, otherwise all failure messages joined with '; '."""
    return df.withColumn("_dq_reason", F.concat_ws("; ", *[rule_failure(r) for r in rules]))


def split_valid_invalid(df: DataFrame):
    """Return (valid_rows, quarantined_rows). Quarantined rows keep the reason and get a rejected-at time."""
    good = df.filter(F.col("_dq_reason") == "").drop("_dq_reason")
    bad = df.filter(F.col("_dq_reason") != "").withColumn("_rejected_at", F.current_timestamp())
    return good, bad


def dedupe_latest(df: DataFrame, keys, order_col="_ingested_at") -> DataFrame:
    """Keep one row per key: the one with the newest order_col."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
