"""Spark checks a metric view's YAML when the view is created, and each query when it is analyzed.

    python examples/06_definition_rules.py

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
spark.read.parquet(f"{data}/sessions.parquet").createOrReplaceTempView("sessions_temp")

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


def attempt(description: str, statement: str) -> None:
    try:
        spark.sql(statement).collect()
        print(f"  accepted  {description}")
    except Exception as exc:  # the error condition is what this example shows
        condition = getattr(exc, "getCondition", lambda: None)() or type(exc).__name__
        print(f"  rejected  {description}\n            {condition}")


def create(definition: str) -> str:
    spark.sql("DROP VIEW IF EXISTS rules_check")
    return f"CREATE VIEW rules_check WITH METRICS LANGUAGE YAML AS $${definition}$$"


print("Definitions")
attempt("the definition as written", create(DEFINITION))
attempt("version: 1.0", create(DEFINITION.replace("version: 0.1", "version: 1.0")))
attempt("a joins key", create(DEFINITION + "joins:\n  - name: users\n    source: users\n"))
attempt(
    "display_name on a dimension",
    create(DEFINITION.replace("expr: region\n", "expr: region\n    display_name: Region\n")),
)
attempt("a temporary view as the source",
        create(DEFINITION.replace("source: sessions", "source: sessions_temp")))
attempt("a measure without an aggregate function",
        create(DEFINITION.replace("COUNT(1)", "CAST(converted AS INT)")))
attempt("a query as the source",
        create(DEFINITION.replace("source: sessions", "source: SELECT * FROM sessions")))

print("Queries")
spark.sql(create(DEFINITION))
attempt("SELECT *", "SELECT * FROM rules_check")
attempt("a measure without MEASURE()", "SELECT total_sessions FROM rules_check")
attempt("a dimension without GROUP BY", "SELECT region, MEASURE(total_sessions) FROM rules_check")
attempt("a source column that is not a dimension",
        "SELECT user_id, MEASURE(total_sessions) FROM rules_check GROUP BY user_id")
attempt("MEASURE() in ORDER BY",
        "SELECT region, MEASURE(total_sessions) AS sessions FROM rules_check"
        " GROUP BY region ORDER BY MEASURE(total_sessions)")
attempt("the measure's alias in ORDER BY",
        "SELECT region, MEASURE(total_sessions) AS sessions FROM rules_check"
        " GROUP BY region ORDER BY sessions")
spark.sql("DROP VIEW IF EXISTS rules_check")
spark.stop()
