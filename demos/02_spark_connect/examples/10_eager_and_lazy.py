"""Spark Connect analyzes plans lazily: errors surface at the action, and views resolve by name.

    python examples/10_eager_and_lazy.py classic    # full pyspark (.venv-full) and a JDK
    python examples/10_eager_and_lazy.py connect    # pyspark-client (.venv) and a server

Blog post §8.
"""
import os
import sys

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

mode = sys.argv[1] if len(sys.argv) > 1 else "connect"
if mode == "classic":
    # PySpark refuses to combine master() with SPARK_REMOTE, so clear it for the Classic session.
    os.environ.pop("SPARK_REMOTE", None)
    spark = SparkSession.builder.master("local[2]").getOrCreate()
else:
    remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
    spark = SparkSession.builder.remote(remote).getOrCreate()
print(f"mode: {mode}")

# 1. A reference to a column that does not exist.
try:
    bad = spark.range(3).select("no_such_column")
except AnalysisException:
    print("invalid column: error raised when the DataFrame is defined")
else:
    try:
        bad.collect()
    except AnalysisException:
        print("invalid column: no error at definition; error raised when the action runs")

# 2. Replacing a temporary view after a DataFrame that reads it has been defined.
spark.range(3).createOrReplaceTempView("t")
before_replace = spark.table("t")
spark.range(7).createOrReplaceTempView("t")
print(f"view t replaced: the earlier DataFrame now counts {before_replace.count()} rows")
spark.stop()
