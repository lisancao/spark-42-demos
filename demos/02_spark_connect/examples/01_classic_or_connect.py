"""The same DataFrame code on Spark Classic and on Spark Connect; only session creation differs.

    python examples/01_classic_or_connect.py classic    # full pyspark (.venv-full) and a JDK
    python examples/01_classic_or_connect.py connect    # pyspark-client (.venv) and a server

Blog post §4 and §6.
"""
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def orders_by_item(spark: SparkSession) -> DataFrame:
    orders = spark.createDataFrame(
        [
            ("metro", "pizza", 24.0),
            ("metro", "sushi", 40.0),
            ("urban", "pizza", 18.0),
            ("urban", "tacos", 12.0),
            ("rural", "pizza", 21.0),
        ],
        "region string, item string, total double",
    )
    return (
        orders.groupBy("item")
        .agg(F.count("*").alias("orders"), F.round(F.sum("total"), 2).alias("revenue"))
        .orderBy(F.desc("orders"), "item")
    )


mode = sys.argv[1] if len(sys.argv) > 1 else "connect"

if mode == "classic":
    # PySpark refuses to combine master() with SPARK_REMOTE, so clear it for the Classic session.
    os.environ.pop("SPARK_REMOTE", None)
    # Local Python workers start "python3" from PATH unless PYSPARK_PYTHON names an interpreter.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = SparkSession.builder.master("local[*]").appName("example-01").getOrCreate()
else:
    remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
    spark = SparkSession.builder.remote(remote).appName("example-01").getOrCreate()

print(f"mode: {mode}, Spark {spark.version}, session class from {type(spark).__module__}")
orders_by_item(spark).show()
spark.stop()

# The function is identical in both modes. Only the line that creates the session changed.
