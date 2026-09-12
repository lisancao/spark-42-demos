"""MEASURE() evaluates a measure's aggregate at the grain of the query that uses it.

    python examples/04_measure_at_every_grain.py      # after examples/03_create_metric_view.py

Blog post §5, "Querying with MEASURE()".
"""
import os

from pyspark.sql import SparkSession

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(remote).getOrCreate()
if not spark.catalog.tableExists("delivery_metrics"):
    raise SystemExit("delivery_metrics does not exist; run examples/03_create_metric_view.py first")

print("The whole month")
spark.sql("""
    SELECT ROUND(MEASURE(conversion_rate), 4) AS conversion_rate,
           MEASURE(active_users) AS active_users,
           MEASURE(total_sessions) AS sessions
    FROM delivery_metrics
""").show()

# ORDER BY refers to the measure by its alias. MEASURE() inside ORDER BY does not resolve.
print("By region")
spark.sql("""
    SELECT region,
           MEASURE(total_sessions) AS sessions,
           ROUND(MEASURE(conversion_rate), 4) AS conversion_rate,
           MEASURE(active_users) AS active_users
    FROM delivery_metrics
    GROUP BY region
    ORDER BY sessions DESC
""").show()

print("By day (first five days)")
spark.sql("""
    SELECT session_date,
           MEASURE(active_users) AS active_users,
           ROUND(MEASURE(conversion_rate), 4) AS conversion_rate
    FROM delivery_metrics
    GROUP BY session_date
    ORDER BY session_date
""").show(5)
spark.stop()
