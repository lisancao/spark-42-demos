"""The sum of daily distinct users counts a user once for every day with a session.

    python examples/02_summing_distinct_counts.py

Blog post §3, "Two Aggregation Errors".
"""
import os

from pyspark.sql import SparkSession

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
data = os.environ.get("DEMO_DATA", "/demo/data")
spark = SparkSession.builder.remote(remote).getOrCreate()

for table in ("sessions", "users"):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} USING parquet LOCATION '{data}/{table}.parquet'"
    )

print("Distinct users by day (first five days)")
spark.sql("""
    SELECT session_date, COUNT(DISTINCT user_id) AS active_users
    FROM sessions
    GROUP BY session_date
    ORDER BY session_date
""").show(5)

# A distinct count of a month is not the sum of the distinct counts of its days.
print("Summing the daily counts, and counting once for the month")
spark.sql("""
    WITH daily AS (
        SELECT session_date, COUNT(DISTINCT user_id) AS active_users
        FROM sessions
        GROUP BY session_date
    )
    SELECT (SELECT SUM(active_users) FROM daily) AS sum_of_daily_active_users,
           (SELECT COUNT(DISTINCT user_id) FROM sessions) AS monthly_active_users,
           (SELECT COUNT(*) FROM users) AS users
""").show()
spark.stop()
