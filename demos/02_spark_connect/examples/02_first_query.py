"""A query built on the client runs on the server, and explain() prints the server's plan.

    python examples/02_first_query.py

Blog post §2, "Observing the Division of Labor".
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(remote).getOrCreate()
print(f"Connected to Spark {spark.version} at {remote}")

orders = spark.createDataFrame(
    [
        ("metro", "pizza", 24.0),
        ("metro", "sushi", 40.0),
        ("urban", "pizza", 18.0),
        ("urban", "tacos", 12.0),
        ("rural", "sushi", 52.0),
    ],
    "region string, item string, total double",
)

by_region = (
    orders.groupBy("region")
    .agg(F.count("*").alias("orders"), F.round(F.avg("total"), 2).alias("avg_ticket"))
    .orderBy(F.desc("avg_ticket"))
)

# The client sends the plan; the server executes it and returns the rows as Arrow batches.
by_region.show()

# The client has no optimizer. Catalyst on the server produced this physical plan.
by_region.explain()
spark.stop()
