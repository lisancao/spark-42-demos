"""A query can still aggregate the results of MEASURE() a second time, and get the same errors.

    python examples/05_what_it_does_not_prevent.py    # after examples/03_create_metric_view.py

Blog post §6, "What a Metric View Does Not Prevent".
"""
import os

from pyspark.sql import SparkSession

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(remote).getOrCreate()
if not spark.catalog.tableExists("delivery_metrics"):
    raise SystemExit("delivery_metrics does not exist; run examples/03_create_metric_view.py first")

print("MEASURE() at the grain of the question")
spark.sql("""
    SELECT MEASURE(active_users) AS monthly_active_users,
           ROUND(MEASURE(conversion_rate), 4) AS conversion_rate
    FROM delivery_metrics
""").show()

# The inner queries are correct at their own grain. The outer queries sum distinct counts and
# average ratios, and Spark runs them as ordinary SQL.
print("Summing daily MEASURE(active_users)")
spark.sql("""
    SELECT SUM(active_users) AS sum_of_daily_active_users
    FROM (
        SELECT session_date, MEASURE(active_users) AS active_users
        FROM delivery_metrics
        GROUP BY session_date
    )
""").show()

print("Averaging regional MEASURE(conversion_rate)")
spark.sql("""
    SELECT ROUND(AVG(conversion_rate), 4) AS average_of_region_rates
    FROM (
        SELECT region, MEASURE(conversion_rate) AS conversion_rate
        FROM delivery_metrics
        GROUP BY region
    )
""").show()
spark.stop()
