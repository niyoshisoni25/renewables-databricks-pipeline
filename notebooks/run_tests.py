# Databricks notebook source
# MAGIC %md
# MAGIC ## Run the unit tests on Databricks
# MAGIC Run this notebook from a **Git folder** so the `src` and `tests` folders are next to it.

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

import sys, os, pytest
sys.dont_write_bytecode = True                       # avoid stale .pyc files in the repo folder
repo_root = os.path.abspath("..")
sys.path.insert(0, repo_root)
result = pytest.main(["-q", "-p", "no:cacheprovider", os.path.join(repo_root, "tests")])
assert result == 0, "Unit tests failed: see the output above"
print("All tests passed")
