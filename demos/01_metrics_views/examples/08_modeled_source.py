"""A metric view over a query that joins tables, with a filter, derived dimensions and ratios.

    python examples/08_modeled_source.py

Blog post §8, "Modeling the Source".
"""
import os

from pyspark.sql import SparkSession

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
data = os.environ.get("DEMO_DATA", "/demo/data")
spark = SparkSession.builder.remote(remote).getOrCreate()

for table in ("sessions", "orders"):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} USING parquet LOCATION '{data}/{table}.parquet'"
    )

DEFINITION = """
version: 0.1
source: >
  SELECT s.session_id, s.user_id, s.region, s.session_date, s.converted,
  o.order_id, o.order_total
  FROM sessions s LEFT JOIN orders o ON s.session_id = o.session_id
filter: region <> 'remote'
dimensions:
  - name: region
    expr: region
  - name: session_date
    expr: session_date
  - name: day_type
    expr: CASE WHEN dayofweek(to_date(session_date)) IN (1, 7) THEN 'weekend' ELSE 'weekday' END
  - name: region_tier
    expr: CASE WHEN region IN ('metro', 'urban') THEN 'core' ELSE 'long_tail' END
measures:
  - name: sessions
    expr: COUNT(1)
  - name: conversion_rate
    expr: SUM(CAST(converted AS INT)) / COUNT(1)
  - name: paying_users
    expr: COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN user_id END)
  - name: orders
    expr: COUNT(order_id)
  - name: revenue
    expr: SUM(order_total)
  - name: aov
    expr: SUM(order_total) / NULLIF(COUNT(order_id), 0)
  - name: arppu
    expr: SUM(order_total)
      / NULLIF(COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN user_id END), 0)
"""

spark.sql("DROP VIEW IF EXISTS order_metrics")
spark.sql(f"CREATE VIEW order_metrics WITH METRICS LANGUAGE YAML AS $${DEFINITION}$$")

# aov is average order value; arppu is average revenue per paying user.
print("The whole month, excluding the remote region")
spark.sql("""
    SELECT MEASURE(orders) AS orders,
           ROUND(MEASURE(revenue), 2) AS revenue,
           ROUND(MEASURE(aov), 2) AS aov,
           ROUND(MEASURE(arppu), 2) AS arppu
    FROM order_metrics
""").show()

print("By region tier and day type")
spark.sql("""
    SELECT region_tier, day_type,
           MEASURE(sessions) AS sessions,
           ROUND(MEASURE(conversion_rate), 4) AS conversion_rate,
           MEASURE(paying_users) AS paying_users,
           ROUND(MEASURE(aov), 2) AS aov,
           ROUND(MEASURE(arppu), 2) AS arppu
    FROM order_metrics
    GROUP BY region_tier, day_type
    ORDER BY region_tier, day_type
""").show()

# Averaging the four slices does not give the month's values.
print("Averages of the four slices")
spark.sql("""
    SELECT ROUND(AVG(aov), 2) AS average_of_slice_aov,
           ROUND(AVG(arppu), 2) AS average_of_slice_arppu
    FROM (
        SELECT region_tier, day_type, MEASURE(aov) AS aov, MEASURE(arppu) AS arppu
        FROM order_metrics
        GROUP BY region_tier, day_type
    )
""").show()
spark.stop()
