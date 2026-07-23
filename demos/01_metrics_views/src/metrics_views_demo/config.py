"""Shared paths + Spark session helper for the metric-views demo.

Connection strategy:
  * default -> Spark Connect against the sandbox 4.2 server (SPARK_REMOTE, sc://localhost:15099)
  * SPARK_LOCAL=1 -> a plain local SparkSession (the "naive lie" half runs on any Spark 4.x;
    metric-view DDL requires the 4.2 server).
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# demos/01_metrics_views/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
SQL_DIR = PROJECT_ROOT / "sql"

TABLES = ("users", "sessions", "orders")

SPARK_REMOTE = os.getenv("SPARK_REMOTE", "sc://localhost:15099")
USE_LOCAL = os.getenv("SPARK_LOCAL", "") == "1"


def get_spark(app_name: str = "metric-views-demo"):
    """Return a SparkSession — Connect remote by default, local if SPARK_LOCAL=1."""
    from pyspark.sql import SparkSession

    if USE_LOCAL:
        return (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    return SparkSession.builder.appName(app_name).remote(SPARK_REMOTE).getOrCreate()


def register_tables(spark) -> None:
    """Load the generated parquet as MANAGED tables: users, sessions, orders.

    Managed (persistent) tables — not temp views — because a Spark 4.2 metric view
    cannot reference a temporary object (INVALID_TEMP_OBJ_REFERENCE). Overwrite each
    run for idempotency.
    """
    missing = [t for t in TABLES if not (DATA_DIR / f"{t}.parquet").exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing generated data for {missing}. "
            f"Run:  python -m metrics_views_demo.generate_data"
        )
    for t in TABLES:
        (
            spark.read.parquet(str(DATA_DIR / f"{t}.parquet"))
            .write.mode("overwrite")
            .saveAsTable(t)
        )
