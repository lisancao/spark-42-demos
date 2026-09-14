"""MEASURE() returns the values of the equivalent hand-written aggregates, at every grain."""
import pytest

import figures


def rows(spark, statement: str) -> list[tuple]:
    return [tuple(row) for row in spark.sql(statement).collect()]


@pytest.fixture(scope="module")
def delivery_metrics(spark, definition):
    yaml = definition("03_create_metric_view.py")
    spark.sql("DROP VIEW IF EXISTS test_delivery_metrics")
    spark.sql(f"CREATE VIEW test_delivery_metrics WITH METRICS LANGUAGE YAML AS $${yaml}$$")
    yield "test_delivery_metrics"
    spark.sql("DROP VIEW IF EXISTS test_delivery_metrics")


def test_whole_month(spark, delivery_metrics):
    measured = rows(spark, f"""
        SELECT MEASURE(total_sessions), MEASURE(converted_sessions),
               MEASURE(conversion_rate), MEASURE(active_users)
        FROM {delivery_metrics}""")
    by_hand = rows(spark, """
        SELECT COUNT(1), SUM(CAST(converted AS INT)),
               SUM(CAST(converted AS INT)) / COUNT(1), COUNT(DISTINCT user_id)
        FROM sessions""")
    assert measured == by_hand
    # The figures are rounded as the examples round them, with Spark's ROUND.
    rounded = rows(spark, f"""
        SELECT MEASURE(total_sessions), ROUND(MEASURE(conversion_rate), 4), MEASURE(active_users)
        FROM {delivery_metrics}""")
    assert rounded == [(figures.SESSIONS, figures.CONVERSION_RATE, figures.MONTHLY_ACTIVE_USERS)]


@pytest.mark.parametrize("grain", ["region", "session_date"])
def test_grain(spark, delivery_metrics, grain):
    measured = rows(spark, f"""
        SELECT {grain}, MEASURE(total_sessions), MEASURE(conversion_rate), MEASURE(active_users)
        FROM {delivery_metrics} GROUP BY {grain} ORDER BY {grain}""")
    by_hand = rows(spark, f"""
        SELECT {grain}, COUNT(1), SUM(CAST(converted AS INT)) / COUNT(1), COUNT(DISTINCT user_id)
        FROM sessions GROUP BY {grain} ORDER BY {grain}""")
    assert measured == by_hand


def test_regions(spark, delivery_metrics):
    measured = rows(spark, f"""
        SELECT region, MEASURE(total_sessions), ROUND(MEASURE(conversion_rate), 4),
               MEASURE(active_users)
        FROM {delivery_metrics} GROUP BY region""")
    expected = {
        region: (sessions, rate, users)
        for region, (sessions, _, rate, users) in figures.REGIONS.items()
    }
    assert {row[0]: row[1:] for row in measured} == expected


def test_measure_results_can_be_aggregated_again(spark, delivery_metrics):
    total = rows(spark, f"""
        SELECT SUM(active_users) FROM (
            SELECT session_date, MEASURE(active_users) AS active_users
            FROM {delivery_metrics} GROUP BY session_date)""")
    average = rows(spark, f"""
        SELECT ROUND(AVG(rate), 4) FROM (
            SELECT region, MEASURE(conversion_rate) AS rate
            FROM {delivery_metrics} GROUP BY region)""")
    assert total == [(figures.SUM_OF_DAILY_ACTIVE_USERS,)]
    assert average == [(figures.AVERAGE_OF_REGION_RATES,)]


def test_modeled_source(spark, definition):
    yaml = definition("08_modeled_source.py")
    spark.sql("DROP VIEW IF EXISTS test_order_metrics")
    spark.sql(f"CREATE VIEW test_order_metrics WITH METRICS LANGUAGE YAML AS $${yaml}$$")
    try:
        totals = rows(spark, """
            SELECT MEASURE(orders), ROUND(MEASURE(revenue), 2),
                   ROUND(MEASURE(aov), 2), ROUND(MEASURE(arppu), 2)
            FROM test_order_metrics""")
        slices = rows(spark, """
            SELECT region_tier, day_type, MEASURE(sessions), ROUND(MEASURE(conversion_rate), 4),
                   MEASURE(paying_users), ROUND(MEASURE(aov), 2), ROUND(MEASURE(arppu), 2)
            FROM test_order_metrics GROUP BY region_tier, day_type""")
        averages = rows(spark, """
            SELECT ROUND(AVG(aov), 2), ROUND(AVG(arppu), 2) FROM (
                SELECT region_tier, day_type, MEASURE(aov) AS aov, MEASURE(arppu) AS arppu
                FROM test_order_metrics GROUP BY region_tier, day_type)""")
    finally:
        spark.sql("DROP VIEW IF EXISTS test_order_metrics")
    assert totals == [(
        figures.MODELED_ORDERS, figures.MODELED_REVENUE, figures.MODELED_AOV, figures.MODELED_ARPPU
    )]
    assert {(tier, day): tuple(rest) for tier, day, *rest in slices} == figures.SLICES
    assert averages == [(figures.AVERAGE_OF_SLICE_AOV, figures.AVERAGE_OF_SLICE_ARPPU)]
