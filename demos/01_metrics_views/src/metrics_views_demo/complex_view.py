"""A COMPLEX metric view — the advanced companion to run_footgun.

    python -m metrics_views_demo.complex_view

Shows the real-world shape of a metric view once you go past the toy case:
  * metric views can't join (this build) -> you model a wide fact table first
  * a global `filter` on the view
  * derived dimensions (CASE / date parts)
  * non-additive measures: ratios, guarded division (NULLIF), conditional distinct counts

Every number is recomputed correctly at whatever grain you group by. Needs the 4.2
sandbox server. All grammar here is verified against it (see sql/04_complex_metric_view.sql).
"""

from __future__ import annotations

import sys

from .config import get_spark, register_tables

WIDE_TABLE_SQL = """
CREATE TABLE session_facts AS
SELECT  se.session_id, se.user_id, se.region, u.home_region,
        se.session_date, se.converted,
        o.order_id, o.order_total
FROM sessions se
LEFT JOIN orders o ON se.session_id = o.session_id
LEFT JOIN users  u ON se.user_id   = u.user_id
"""

METRIC_VIEW_YAML = """version: 0.1
source: session_facts
filter: region <> 'remote'
dimensions:
  - name: region
    expr: region
  - name: home_region
    expr: home_region
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
  - name: active_users
    expr: COUNT(DISTINCT user_id)
  - name: paying_users
    expr: COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN user_id END)
  - name: orders
    expr: COUNT(order_id)
  - name: revenue
    expr: SUM(order_total)
  - name: aov
    expr: SUM(order_total) / NULLIF(COUNT(order_id), 0)
  - name: arppu
    expr: SUM(order_total) / NULLIF(COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN user_id END), 0)
"""


def build(spark) -> None:
    """Model the wide fact table and (re)create the complex metric view."""
    spark.sql("DROP TABLE IF EXISTS session_facts")
    spark.sql(WIDE_TABLE_SQL)
    spark.sql("DROP VIEW IF EXISTS delivery_metrics_plus")
    spark.sql(
        f"CREATE VIEW delivery_metrics_plus WITH METRICS LANGUAGE YAML AS $${METRIC_VIEW_YAML}$$"
    )


def main() -> int:
    try:
        spark = get_spark()
    except Exception as exc:  # noqa: BLE001
        print(f"Could not start Spark: {exc}")
        return 2

    try:
        register_tables(spark)
        try:
            build(spark)
        except Exception as exc:  # noqa: BLE001
            print("Complex metric view could not be built (needs the 4.2 sandbox server):")
            print(f"  {type(exc).__name__}: {str(exc).splitlines()[0]}")
            return 1

        print("\nWide fact `session_facts` built; `delivery_metrics_plus` created.")
        print(f"  rows: {spark.sql('SELECT COUNT(*) FROM session_facts').collect()[0][0]:,}")

        print("\nOverall — AOV/ARPPU recomputed at this grain (NOT averaged from the slices):")
        spark.sql(
            "SELECT ROUND(MEASURE(aov), 2) aov, ROUND(MEASURE(arppu), 2) arppu, "
            "MEASURE(orders) orders, ROUND(MEASURE(revenue), 2) revenue "
            "FROM delivery_metrics_plus"
        ).show(truncate=False)

        print("By region_tier x day_type — two derived dims, filter applied, ratios re-derived:")
        spark.sql(
            "SELECT region_tier, day_type, MEASURE(sessions) sessions, "
            "ROUND(MEASURE(conversion_rate), 4) conversion_rate, MEASURE(paying_users) paying_users, "
            "ROUND(MEASURE(aov), 2) aov, ROUND(MEASURE(arppu), 2) arppu "
            "FROM delivery_metrics_plus GROUP BY region_tier, day_type "
            "ORDER BY region_tier, day_type"
        ).show(truncate=False)

        print("The footgun this kills: AVG(aov) across those 4 rows ≠ the overall AOV above.")
        print("A metric view makes the wrong rollup impossible; the right one is the only option.")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
