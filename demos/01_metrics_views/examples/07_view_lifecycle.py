"""Replacing, renaming, inspecting and dropping a metric view in the session catalog.

    python examples/07_view_lifecycle.py

Blog post §7, "Definition Rules and Lifecycle".
"""
import logging
import os

from pyspark.sql import SparkSession

# The client also logs each failed query, with its plan, at ERROR level. The example prints the
# error condition instead.
logging.getLogger("SQLQueryContextLogger").setLevel(logging.CRITICAL)

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
data = os.environ.get("DEMO_DATA", "/demo/data")
spark = SparkSession.builder.remote(remote).getOrCreate()

spark.sql(f"CREATE TABLE IF NOT EXISTS sessions USING parquet LOCATION '{data}/sessions.parquet'")
for name in ("lifecycle_metrics", "lifecycle_metrics_v2"):
    spark.sql(f"DROP VIEW IF EXISTS {name}")

DEFINITION = """
version: 0.1
source: sessions
dimensions:
  - name: region
    expr: region
measures:
  - name: total_sessions
    expr: COUNT(1)
"""
REVISED = DEFINITION + "  - name: active_users\n    expr: COUNT(DISTINCT user_id)\n"


def run(label: str, statement: str) -> list:
    print(label)
    try:
        rows = spark.sql(statement).collect()
    except Exception as exc:  # the error condition is what this example shows
        print(f"  {getattr(exc, 'getCondition', lambda: None)() or type(exc).__name__}")
        return []
    for row in rows:
        print("  " + " | ".join(str(value) for value in row))
    print("  ok")
    return rows


run("CREATE VIEW lifecycle_metrics",
    f"CREATE VIEW lifecycle_metrics WITH METRICS LANGUAGE YAML AS $${DEFINITION}$$")
run("CREATE OR REPLACE VIEW lifecycle_metrics, adding active_users",
    f"CREATE OR REPLACE VIEW lifecycle_metrics WITH METRICS LANGUAGE YAML AS $${REVISED}$$")
run("SHOW CREATE TABLE lifecycle_metrics", "SHOW CREATE TABLE lifecycle_metrics")

# DESCRIBE TABLE EXTENDED returns the YAML as the View Text row.
print("DESCRIBE TABLE EXTENDED lifecycle_metrics, rows Type and View Text")
details = spark.sql("DESCRIBE TABLE EXTENDED lifecycle_metrics").collect()
for row in details:
    if row.col_name == "Type":
        print(f"  Type: {row.data_type}")
    elif row.col_name == "View Text":
        print("  View Text:")
        for line in row.data_type.strip().splitlines():
            print(f"    {line}")

run("ALTER VIEW lifecycle_metrics RENAME TO lifecycle_metrics_v2",
    "ALTER VIEW lifecycle_metrics RENAME TO lifecycle_metrics_v2")
run("SELECT MEASURE(total_sessions) FROM lifecycle_metrics_v2",
    "SELECT MEASURE(total_sessions) FROM lifecycle_metrics_v2")

# ALTER VIEW ... AS replaces the YAML with a SELECT statement, and the view stops working.
run("ALTER VIEW lifecycle_metrics_v2 AS SELECT region FROM sessions",
    "ALTER VIEW lifecycle_metrics_v2 AS SELECT region FROM sessions")
run("SELECT MEASURE(total_sessions) FROM lifecycle_metrics_v2",
    "SELECT MEASURE(total_sessions) FROM lifecycle_metrics_v2")

run("DROP VIEW lifecycle_metrics_v2", "DROP VIEW lifecycle_metrics_v2")
spark.stop()
