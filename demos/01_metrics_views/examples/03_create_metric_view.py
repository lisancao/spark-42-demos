"""A metric view defines dimensions and measures once, in YAML, on top of a table.

    python examples/03_create_metric_view.py

Blog post §4, "Defining a Metric View".
"""
import os

from pyspark.sql import SparkSession

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
data = os.environ.get("DEMO_DATA", "/demo/data")
spark = SparkSession.builder.remote(remote).getOrCreate()

spark.sql(f"CREATE TABLE IF NOT EXISTS sessions USING parquet LOCATION '{data}/sessions.parquet'")

DEFINITION = """
version: 0.1
source: sessions
dimensions:
  - name: region
    expr: region
  - name: session_date
    expr: session_date
measures:
  - name: total_sessions
    expr: COUNT(1)
  - name: converted_sessions
    expr: SUM(CAST(converted AS INT))
  - name: conversion_rate
    expr: SUM(CAST(converted AS INT)) / COUNT(1)
  - name: active_users
    expr: COUNT(DISTINCT user_id)
"""

# CREATE OR REPLACE does not replace an existing metric view in the session catalog, so the
# example drops the view first and can run more than once.
spark.sql("DROP VIEW IF EXISTS delivery_metrics")
spark.sql(f"CREATE VIEW delivery_metrics WITH METRICS LANGUAGE YAML AS $${DEFINITION}$$")

print("Columns of delivery_metrics")
spark.sql("DESCRIBE TABLE delivery_metrics").show()

print("Catalog entry")
(
    spark.sql("DESCRIBE TABLE EXTENDED delivery_metrics")
    .where("col_name IN ('Type', 'Language', 'Table Properties')")
    .show(truncate=False)
)
spark.stop()
