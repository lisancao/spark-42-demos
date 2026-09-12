"""The average of per-region conversion rates differs from total conversions divided by sessions.

    python examples/01_average_of_ratios.py

Blog post §3, "Two Aggregation Errors".
"""
import os

from pyspark.sql import SparkSession

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
data = os.environ.get("DEMO_DATA", "/demo/data")
spark = SparkSession.builder.remote(remote).getOrCreate()

# An external table reads the Parquet file where it is. The path is resolved on the server.
spark.sql(f"CREATE TABLE IF NOT EXISTS sessions USING parquet LOCATION '{data}/sessions.parquet'")

print("Conversion rate by region")
spark.sql("""
    SELECT region,
           COUNT(*) AS sessions,
           SUM(CAST(converted AS INT)) AS converted,
           ROUND(AVG(CAST(converted AS DOUBLE)), 4) AS conversion_rate
    FROM sessions
    GROUP BY region
    ORDER BY sessions DESC
""").show()

# Averaging the five rates gives each region the same weight: 200 remote sessions count as much
# as 50,000 metro sessions. Dividing the totals weights each region by its sessions.
print("Two ways to combine the regional rates")
spark.sql("""
    SELECT ROUND(AVG(conversion_rate), 4) AS average_of_region_rates,
           ROUND(SUM(converted) / SUM(sessions), 4) AS converted_over_sessions,
           ROUND(AVG(conversion_rate) / (SUM(converted) / SUM(sessions)), 1) AS ratio
    FROM (
        SELECT region,
               COUNT(*) AS sessions,
               SUM(CAST(converted AS INT)) AS converted,
               AVG(CAST(converted AS DOUBLE)) AS conversion_rate
        FROM sessions
        GROUP BY region
    )
""").show()
spark.stop()
