"""Connection helper. Spark Connect is remote-only by design — one connection string."""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

SPARK_REMOTE = os.getenv("SPARK_REMOTE", "sc://localhost:15099")


def get_spark(app_name: str = "spark-connect-demo"):
    """A thin Spark Connect client. No local Spark runtime — just the wheel + a URL."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).remote(SPARK_REMOTE).getOrCreate()
