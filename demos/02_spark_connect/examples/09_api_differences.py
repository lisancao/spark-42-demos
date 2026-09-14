"""Spark Connect has no SparkContext, RDDs or JVM access; observe() and 4.2 additions fill gaps.

    python examples/09_api_differences.py

Blog post §7.
"""
import os

from pyspark.errors import PySparkException
from pyspark.sql import Observation, SparkSession
from pyspark.sql import functions as F

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(remote).getOrCreate()
df = spark.range(25)

# 1. APIs that reach into the driver JVM are not available.
for name, access in [
    ("spark.sparkContext", lambda: spark.sparkContext),
    ("spark._jvm", lambda: spark._jvm),
    ("df.rdd", lambda: df.rdd),
]:
    try:
        access()
    except PySparkException as exc:
        print(f"{name:<20} {str(exc).splitlines()[0]}")

# 2. Count rows during an action with observe(), which replaces an accumulator for this purpose.
observation = Observation("rows")
df.observe(observation, F.count(F.lit(1)).alias("n")).collect()
print(f"observed rows: {observation.get['n']}")

# 3. APIs available to the Connect client as of Spark 4.2.
spark.createDataFrame([("a",), ("b",), ("c",)], "letter string").zipWithIndex().show()
json_lines = spark.createDataFrame([('{"city": "Toronto", "orders": 3}',)], "value string")
spark.read.json(json_lines).show()
print("emptyDataFrame rows:", spark.emptyDataFrame("a int").count())
spark.stop()
