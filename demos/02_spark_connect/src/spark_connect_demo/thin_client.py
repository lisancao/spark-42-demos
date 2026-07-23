"""Thin-client demo: identical DataFrame code, executed on a remote server over gRPC.

    python -m spark_connect_demo.thin_client

The point: there is no local Spark here. `pip install pyspark[connect]` is a small pure-Python
wheel; the JVM, the executors, the data all live on the server. The DataFrame code below is
byte-for-byte what you'd write against a local SparkSession — only the connection string moved.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from .config import SPARK_REMOTE, get_spark


def main() -> int:
    spark = get_spark()
    print(f"Connected over Spark Connect to: {SPARK_REMOTE}")
    print(f"Server Spark version:            {spark.version}")
    print("(No local Spark/JVM in this process — this is a gRPC + Arrow client.)\n")

    # --- ordinary DataFrame code — nothing Connect-specific about it --------------
    orders = spark.createDataFrame(
        [
            ("metro", "pizza", 24.0),
            ("metro", "sushi", 40.0),
            ("urban", "pizza", 18.0),
            ("urban", "tacos", 12.0),
            ("rural", "sushi", 52.0),
        ],
        schema="region string, item string, total double",
    )

    by_region = (
        orders.groupBy("region")
        .agg(
            F.count("*").alias("orders"),
            F.round(F.avg("total"), 2).alias("avg_ticket"),
        )
        .orderBy(F.desc("avg_ticket"))
    )

    print("Aggregation computed on the remote server, results streamed back as Arrow:")
    by_region.show(truncate=False)

    # results come back as ordinary Python objects (Arrow under the hood)
    top = by_region.first()
    print(f"Top region by ticket: {top['region']} (${top['avg_ticket']})")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
