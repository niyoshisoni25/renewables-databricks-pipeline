# Renewables Data Pipeline on Databricks

A small, metadata-driven data pipeline on Azure Databricks / Delta Lake using PySpark and SQL, with unit tests and CI.
It processes **synthetic** wind turbine data (no real company data) through bronze, silver and gold layers.
Built in October 2026 to learn the Databricks stack hands-on.

## What it does
- **Landing:** generates CSV files, with deliberately bad rows and re-sent duplicates.
- **Bronze:** Auto Loader / Structured Streaming loads only new files, as raw text plus lineage (`_source_file`, `_ingested_at`).
- **Silver:** typing, data quality rules from a config, quarantine table with reasons, de-duplication, Delta MERGE upserts.
- **Gold:** site daily performance, turbine underperformance flag, maintenance summary, data quality summary.
- **Quality guard:** the run fails if a source's reject rate is above a threshold.
- **Tests and CI:** the data quality logic is in `src/renewables/dq.py`, covered by pytest, and run by GitHub Actions on every push.

## Repository layout
```
notebooks/renewables_medallion_v3.py   main pipeline notebook
notebooks/run_tests.py                 runs the unit tests on Databricks
src/renewables/dq.py                   reusable data quality functions
tests/test_dq.py                       unit tests
sql/dashboard_queries.sql              queries behind the dashboard
.github/workflows/ci.yml               GitHub Actions: runs tests on every push
```

## How to run
1. Create a free Databricks account (Free Edition) and create a Git folder from this repository.
2. Open `notebooks/renewables_medallion_v3.py` and Run all. Run it again: bronze should load 0 new rows.
3. Optionally schedule it as a Databricks Job and build a dashboard from `sql/dashboard_queries.sql`.
4. Tests locally: `pip install -r requirements.txt` then `python -m pytest -q` (needs Java for Spark).

## What I learned / what I'd do next
- (Write 3 to 4 honest bullets in your own words.)
- Next: Unity Catalog permissions and lineage, Databricks Asset Bundles for deployment, true continuous streaming.
