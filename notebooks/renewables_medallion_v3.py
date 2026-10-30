# Databricks notebook source
# MAGIC %md
# MAGIC # Renewables data pipeline v3: Auto Loader / Structured Streaming bronze, tested DQ module, metadata-driven silver and gold
# MAGIC
# MAGIC **What this does** (all data is synthetic wind turbine telemetry, made up for learning):
# MAGIC 1. **Landing**: generates three daily batches of CSV files (with deliberately bad rows and a re-delivered duplicate).
# MAGIC 2. **Bronze**: a *metadata-driven* loop ingests every source listed in `SOURCES` as raw strings plus lineage columns. Re-running does not load the same file twice.
# MAGIC 3. **Silver**: the same config drives typing, de-duplication, data quality rules and an upsert (MERGE). Bad rows go to a quarantine table with the reason.
# MAGIC 4. **Gold**: business-ready daily site performance (energy, capacity factor, availability) and a data quality summary, ready for Power BI / Databricks SQL.
# MAGIC
# MAGIC Runs unchanged on Databricks (Free Edition / serverless) and locally with `pyspark` + `delta-spark`.

# COMMAND ----------

import os, sys, shutil
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F, Window
from delta.tables import DeltaTable

# Reusable, unit-tested data quality helpers live in src/renewables/dq.py (repo root = one folder above this notebook)
try:    # on Databricks: work out the repo root from this notebook's own path (notebooks/ sits one level below it)
    _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _root = "/Workspace" + os.path.dirname(os.path.dirname(_nb))
except NameError:   # running locally
    _root = os.path.abspath("..")
sys.path.insert(0, _root)
if not os.path.isdir(os.path.join(_root, "src", "renewables")):
    raise RuntimeError(f"Cannot find src/renewables under {_root}. Open this notebook from a Databricks Git folder "
                       "(Workspace > Create > Git folder), not from a single imported file.")
from src.renewables.dq import add_dq_reason, cast_columns, dedupe_latest, split_valid_invalid

IS_DBX = "DATABRICKS_RUNTIME_VERSION" in os.environ

if IS_DBX:
    CATALOG, SCHEMA = "workspace", "renewables_demo"      # change CATALOG if yours differs
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
    LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
    CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/landing/_checkpoints"   # streaming progress is remembered here
else:
    BASE = os.path.abspath("./lakehouse")
    LANDING = os.path.abspath("./landing")
    CHECKPOINTS = os.path.abspath("./checkpoints")
    def make_builder():
        return (SparkSession.builder.master("local[2]").appName("renewables_demo")
                .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4"))
    try:   # Delta locally if the jars can be fetched; otherwise fall back to Parquet (logic test only)
        from delta import configure_spark_with_delta_pip
        spark = configure_spark_with_delta_pip(make_builder().config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")).getOrCreate()
        USE_DELTA = True
    except Exception:
        os.environ.pop("PYSPARK_SUBMIT_ARGS", None); spark = make_builder().getOrCreate(); USE_DELTA = False
    spark.sparkContext.setLogLevel("ERROR")
if IS_DBX: USE_DELTA = True

# --- small storage helpers so the same code runs on Databricks (tables) and locally (Delta paths) ---
def _name(layer, name): return f"{CATALOG}.{SCHEMA}.{layer}_{name}"
def _path(layer, name): return f"{BASE}/{layer}/{name}"

def table_exists(layer, name):
    if IS_DBX: return spark.catalog.tableExists(_name(layer, name))
    return DeltaTable.isDeltaTable(spark, _path(layer, name)) if USE_DELTA else os.path.isdir(_path(layer, name))

def read_table(layer, name):
    if IS_DBX: return spark.table(_name(layer, name))
    return spark.read.format("delta" if USE_DELTA else "parquet").load(_path(layer, name))

def write_table(df, layer, name, mode="overwrite"):
    if IS_DBX or USE_DELTA:
        w = df.write.format("delta").mode(mode)
        w = w.option("overwriteSchema", "true") if mode == "overwrite" else w.option("mergeSchema", "true")
        w.saveAsTable(_name(layer, name)) if IS_DBX else w.save(_path(layer, name))
    else:                                       # local Parquet fallback (no Delta jars available)
        df.write.mode(mode).parquet(_path(layer, name) + ("" if mode == "append" else "_tmp"))
        if mode == "overwrite":
            shutil.rmtree(_path(layer, name), ignore_errors=True); os.rename(_path(layer, name) + "_tmp", _path(layer, name))

def upsert(df, layer, name, keys):
    """MERGE: update rows that already exist (same key), insert new ones."""
    if not table_exists(layer, name):
        write_table(df, layer, name); return
    if IS_DBX or USE_DELTA:
        target = DeltaTable.forName(spark, _name(layer, name)) if IS_DBX else DeltaTable.forPath(spark, _path(layer, name))
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        target.alias("t").merge(df.alias("s"), cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:                                       # emulate MERGE: union, keep the newest row per key
        w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
        merged = read_table(layer, name).unionByName(df).withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        merged.cache().count(); write_table(merged, layer, name)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuration = the metadata that drives the pipeline
# MAGIC Add a new source by adding one entry here. No new pipeline code needed. That is the idea behind a metadata-driven pipeline.

# COMMAND ----------

SOURCES = [
    {
        "name": "turbine_readings",
        "file_glob": "turbine_readings_*.csv",
        "keys": ["reading_id"],
        "columns": {"reading_id": "string", "turbine_id": "string", "reading_ts": "timestamp",
                    "wind_speed_ms": "double", "power_kw": "double", "status": "string"},
        "rules": [  # (rule type, column, extra args)
            ("not_null", "reading_id"), ("not_null", "turbine_id"), ("not_null", "reading_ts"),
            ("range", "wind_speed_ms", 0, 40), ("range", "power_kw", 0, 4000),
            ("allowed", "status", ["RUNNING", "STOPPED", "MAINTENANCE"]),
        ],
    },
    {
        "name": "maintenance_events",
        "file_glob": "maintenance_events_*.csv",
        "keys": ["event_id"],
        "columns": {"event_id": "string", "turbine_id": "string", "start_ts": "timestamp",
                    "end_ts": "timestamp", "event_type": "string"},
        "rules": [("not_null", "event_id"), ("not_null", "turbine_id"), ("not_null", "start_ts"), ("not_null", "end_ts"),
                  ("allowed", "event_type", ["PLANNED", "UNPLANNED"])],
    },
    {
        "name": "turbine_master",
        "file_glob": "turbine_master_*.csv",
        "keys": ["turbine_id"],
        "columns": {"turbine_id": "string", "site": "string", "capacity_kw": "double", "commissioned": "date"},
        "rules": [("not_null", "turbine_id"), ("not_null", "site"), ("range", "capacity_kw", 1, 20000)],
    },
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Landing: generate synthetic source files (3 daily batches)

# COMMAND ----------

def generate_maintenance(landing_dir):
    rng = np.random.default_rng(11)
    rows = []
    for i in range(1, 21):                                   # 20 valid maintenance events
        start = pd.Timestamp("2026-10-01") + pd.Timedelta(hours=int(rng.integers(0, 60)))
        rows.append({"event_id": f"M{i:03d}", "turbine_id": f"T{int(rng.integers(1, 13)):03d}",
                     "start_ts": start.strftime("%Y-%m-%d %H:%M:%S"),
                     "end_ts": (start + pd.Timedelta(hours=int(rng.integers(2, 18)))).strftime("%Y-%m-%d %H:%M:%S"),
                     "event_type": str(rng.choice(["PLANNED", "UNPLANNED"], p=[0.6, 0.4]))})
    rows.append({"event_id": "M021", "turbine_id": None, "start_ts": "2026-10-03 06:00:00", "end_ts": "2026-10-03 07:00:00", "event_type": "PLANNED"})     # bad: no turbine
    rows.append({"event_id": "M022", "turbine_id": "T001", "start_ts": "2026-10-03 06:00:00", "end_ts": "2026-10-03 07:00:00", "event_type": "INSPECTION"})  # bad: unknown type
    pd.DataFrame(rows).to_csv(f"{landing_dir}/maintenance_events_20261003.csv", index=False)

def generate_landing(landing_dir, seed=7):
    os.makedirs(landing_dir, exist_ok=True)
    rng = np.random.default_rng(seed)
    sites = {"Highland Ridge": 5, "Glen Coastal": 4, "Border Hills": 3}   # fictional sites
    master, tid = [], 1
    for site, n in sites.items():
        for _ in range(n):
            master.append({"turbine_id": f"T{tid:03d}", "site": site, "capacity_kw": 3600, "commissioned": "2023-05-01"})
            tid += 1
    master_df = pd.DataFrame(master)
    master_df.to_csv(f"{landing_dir}/turbine_master_20261001.csv", index=False)

    prev = None
    for day in range(3):
        date = pd.Timestamp("2026-10-01") + pd.Timedelta(days=day)
        rows = []
        for t in master_df.turbine_id:
            for h in range(24):
                ws = float(np.clip(rng.normal(9, 4), 0, 30))
                status = rng.choice(["RUNNING", "STOPPED", "MAINTENANCE"], p=[0.93, 0.04, 0.03])
                power = 0.0 if status != "RUNNING" or ws < 3 else float(min(3600, 3600 * ((ws - 3) / 9) ** 3 if ws < 12 else 3600))
                if t == "T007" and day == 1 and status == "RUNNING" and ws >= 12: power *= 0.6   # simulated fault on one turbine
                rows.append({"reading_id": f"{t}-{(date + pd.Timedelta(hours=h)).strftime('%Y%m%d%H')}",
                             "turbine_id": t, "reading_ts": (date + pd.Timedelta(hours=h)).strftime("%Y-%m-%d %H:%M:%S"),
                             "wind_speed_ms": round(ws, 2), "power_kw": round(power, 1), "status": status})
        df = pd.DataFrame(rows)
        # deliberately dirty data
        bad = rng.choice(df.index, 8, replace=False)
        df.loc[bad[0:2], "turbine_id"] = None                 # missing key field
        df.loc[bad[2:4], "wind_speed_ms"] = -5                # impossible value
        df.loc[bad[4:6], "power_kw"] = 99999                  # sensor spike
        df.loc[bad[6:8], "status"] = "UNKNOWN"                # unexpected category
        if prev is not None:                                   # re-delivered rows from yesterday (duplicates)
            df = pd.concat([df, prev.sample(10, random_state=day)], ignore_index=True)
        df.to_csv(f"{landing_dir}/turbine_readings_{date.strftime('%Y%m%d')}.csv", index=False)
        prev = df.dropna(subset=["turbine_id"]).copy()

os.makedirs(LANDING, exist_ok=True)
if not any(f.startswith("turbine_readings_") for f in os.listdir(LANDING)):
    generate_landing(LANDING)
if not any(f.startswith("maintenance_events_") for f in os.listdir(LANDING)):
    generate_maintenance(LANDING)
print("Landing files:", sorted(os.listdir(LANDING)))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Bronze: raw, as-received, plus lineage, loaded incrementally with Auto Loader / Structured Streaming

# COMMAND ----------

def stream_to_bronze(src):
    """Incrementally load NEW files only, using Auto Loader (Databricks) or the plain file stream source (local test).

    The checkpoint remembers which files were already processed, so this replaces our manual 'skip loaded files' join.
    trigger(availableNow=True) = process everything that is new right now, then stop (incremental batch, no cluster left running).
    """
    schema = ", ".join(f"{c} string" for c in src["columns"])          # bronze stays all-text
    reader = spark.readStream
    reader = reader.format("cloudFiles").option("cloudFiles.format", "csv") if IS_DBX else reader.format("csv")
    df = (reader.option("header", True).schema(schema).load(f"{LANDING}/{src['file_glob']}")
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp()))
    writer = (df.writeStream.option("checkpointLocation", f"{CHECKPOINTS}/{src['name']}")
              .trigger(availableNow=True))
    if IS_DBX:
        q = writer.toTable(_name("bronze", src["name"]))
    else:
        q = writer.format("delta" if USE_DELTA else "parquet").start(_path("bronze", src["name"]))
    q.awaitTermination()
    return sum(p["numInputRows"] for p in q.recentProgress)

for s_ in SOURCES:
    print(f"bronze.{s_['name']}: loaded {stream_to_bronze(s_)} new rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Silver: typed, validated, de-duplicated (rules come from the config)

# COMMAND ----------

def build_silver(src):
    bronze = read_table("bronze", src["name"])
    typed = cast_columns(bronze, src["columns"], keep=("_source_file", "_ingested_at"))
    checked = add_dq_reason(typed, src["rules"])
    good, bad = split_valid_invalid(checked)
    deduped = dedupe_latest(good, src["keys"])
    upsert(deduped, "silver", src["name"], src["keys"])
    write_table(bad, "silver", f"{src['name']}_quarantine")                   # rebuilt each run from bronze
    return typed.count(), good.count(), bad.count(), deduped.count()

dq_rows = []
for s in SOURCES:
    total, good, bad, uniq = build_silver(s)
    print(f"silver.{s['name']}: {total} bronze rows -> {bad} quarantined, {good - uniq} duplicates removed, {uniq} kept")
    dq_rows.append((s["name"], total, bad, good - uniq, uniq))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Gold: business-ready tables for Power BI / Databricks SQL

# COMMAND ----------

readings, master = read_table("silver", "turbine_readings"), read_table("silver", "turbine_master")
site_daily = (readings.join(master, "turbine_id")
    .withColumn("reading_date", F.to_date("reading_ts"))
    .groupBy("site", "reading_date")
    .agg(F.round(F.sum("power_kw") / 1000, 2).alias("energy_mwh"),
         F.round(F.sum("power_kw") / F.sum("capacity_kw"), 4).alias("capacity_factor"),
         F.round(F.avg(F.when(F.col("status") == "RUNNING", 1).otherwise(0)), 4).alias("availability"),
         F.round(F.avg("wind_speed_ms"), 2).alias("avg_wind_speed_ms"),
         F.countDistinct("turbine_id").alias("turbines_reporting"))
    .orderBy("site", "reading_date"))
write_table(site_daily, "gold", "site_daily_performance")

dq = spark.createDataFrame(dq_rows, "source string, bronze_rows long, quarantined long, duplicates_removed long, silver_rows long") \
          .withColumn("reject_rate", F.round(F.col("quarantined") / F.col("bronze_rows"), 4)) \
          .withColumn("run_at", F.current_timestamp())
write_table(dq, "gold", "data_quality_summary", mode="append")

# --- EXTENSION A: underperformance flag (simple rule-based anomaly detection) ---
# A turbine that is RUNNING in strong wind (>= 12 m/s, rated-power territory) but produces < 80% of its capacity is suspicious.
RATED_WIND, UNDERPERF_THRESHOLD = 12.0, 0.8
turbine_daily = (readings.join(master, "turbine_id")
    .withColumn("reading_date", F.to_date("reading_ts"))
    .withColumn("strong_wind_running", (F.col("status") == "RUNNING") & (F.col("wind_speed_ms") >= RATED_WIND))
    .withColumn("underperforming", F.col("strong_wind_running") & (F.col("power_kw") < UNDERPERF_THRESHOLD * F.col("capacity_kw")))
    .groupBy("site", "turbine_id", "reading_date")
    .agg(F.sum(F.col("strong_wind_running").cast("int")).alias("strong_wind_hours"),
         F.sum(F.col("underperforming").cast("int")).alias("underperforming_hours"),
         F.round(F.sum("power_kw") / 1000, 2).alias("energy_mwh"))
    .withColumn("underperformance_rate", F.round(F.col("underperforming_hours") / F.when(F.col("strong_wind_hours") > 0, F.col("strong_wind_hours")), 4)))
write_table(turbine_daily, "gold", "turbine_daily_performance")

# --- EXTENSION B: maintenance summary per site (third source joined to the master data) ---
events = read_table("silver", "maintenance_events").join(master, "turbine_id")
site_maintenance = (events.withColumn("duration_hours", (F.col("end_ts").cast("long") - F.col("start_ts").cast("long")) / 3600)
    .groupBy("site")
    .agg(F.count("*").alias("events"),
         F.sum(F.when(F.col("event_type") == "UNPLANNED", 1).otherwise(0)).alias("unplanned_events"),
         F.round(F.sum("duration_hours"), 1).alias("downtime_hours")))
write_table(site_maintenance, "gold", "site_maintenance_summary")

site_daily.show(truncate=False)
read_table("gold", "data_quality_summary").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5b. Data quality guard (EXTENSION C)
# MAGIC If too many rows are rejected, fail the run loudly instead of publishing questionable numbers.

# COMMAND ----------

MAX_REJECT_RATE = 0.10
worst = dq.agg(F.max("reject_rate")).first()[0]
if worst is not None and worst > MAX_REJECT_RATE:
    raise Exception(f"Data quality guard tripped: reject rate {worst:.1%} is above {MAX_REJECT_RATE:.0%}")
print(f"Data quality guard passed (worst reject rate {worst:.1%}, limit {MAX_REJECT_RATE:.0%})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Check the pipeline is repeatable
# MAGIC Re-run the notebook: bronze should report **0 new rows** (idempotent) and silver counts should not grow (MERGE upserts, no duplicates).
# MAGIC
# MAGIC **Next steps to extend it:** Auto Loader / Structured Streaming for continuous ingestion, a Databricks Job to schedule it, a Databricks SQL dashboard or Power BI connection on the gold tables, and unit tests plus Git-based deployment.
