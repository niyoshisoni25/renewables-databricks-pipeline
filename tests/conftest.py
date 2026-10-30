import os, sys
import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))   # make `src` importable


@pytest.fixture(scope="session")
def spark():
    existing = SparkSession.getActiveSession()          # on Databricks a session already exists: reuse it, never stop it
    if existing is not None:
        yield existing
        return
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()
