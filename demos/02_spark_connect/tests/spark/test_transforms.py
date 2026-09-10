"""Tests for pipeline/lib/transforms.py, which run unchanged on Spark Classic and Spark Connect."""
import json

import pytest
from lib import transforms as T
from pyspark.sql import functions as F

BODY_1 = json.dumps({"brand_id": 1, "lat": 43.70, "lng": -79.40, "total": 20.5})
BODY_2 = json.dumps({"brand_id": 2, "lat": 45.52, "lng": -73.58, "total": 31.0})
EVENTS = [
    # order o1 is delivered on Sunday 2026-09-06; order o2 is created on Monday and not delivered
    ("e1", "order_created", "2026-09-06T12:00:00", "o1", 1, BODY_1),
    ("e2", "order_ready", "2026-09-06T12:15:00", "o1", 1, BODY_1),
    ("e3", "driver_picked_up", "2026-09-06T12:20:00", "o1", 1, BODY_1),
    ("e4", "delivered", "2026-09-06T12:45:00", "o1", 1, BODY_1),
    ("e5", "order_created", "2026-09-07T09:00:00", "o2", 2, BODY_2),
    ("e6", "order_created", "2026-09-07T09:05:00", None, 1, "{}"),
]
STORES = [(1, "Toronto", 43.6532, -79.3832), (2, "Montreal", 45.5019, -73.5674)]
BRANDS = [(1, "Night Noodle"), (2, "Green Bowl")]


@pytest.fixture(scope="module")
def enriched(spark):
    events = T.parse_events(spark.createDataFrame(EVENTS, T.EVENT_SCHEMA))
    stores = spark.createDataFrame(STORES, T.STORE_SCHEMA)
    return T.enrich_orders(events, stores, T.distance_udf())


def test_parse_events_reads_iso_timestamps(spark):
    events = T.parse_events(spark.createDataFrame(EVENTS, T.EVENT_SCHEMA))
    hour = events.filter("event_id = 'e1'").select(F.hour("event_timestamp")).collect()[0][0]
    assert hour == 12


def test_enrich_orders_drops_events_without_an_order_id(enriched):
    assert enriched.count() == 5


def test_enrich_orders_adds_body_store_and_distance_columns(enriched):
    row = enriched.filter("event_id = 'e1'").collect()[0]
    assert (row["brand_id"], row["order_total"], row["city"]) == (1, 20.5, "Toronto")
    assert row["is_weekend"] is True
    assert 5 < row["delivery_km"] < 6


def test_order_lifecycle_keeps_delivered_orders_with_durations(enriched):
    rows = T.order_lifecycle(enriched).collect()
    assert [row["order_id"] for row in rows] == ["o1"]
    assert (rows[0]["prep_min"], rows[0]["delivery_min"], rows[0]["total_min"]) == (15, 25, 45)


def test_delivery_performance_summarizes_by_city(enriched):
    lifecycle = T.order_lifecycle(enriched)
    (row,) = T.delivery_performance(lifecycle, enriched).collect()
    assert (row["city"], row["delivered_orders"], row["median_total_min"]) == ("Toronto", 1, 45)


def test_brand_summary_counts_created_orders_and_adds_names(spark, enriched):
    brands = spark.createDataFrame(BRANDS, T.BRAND_SCHEMA)
    rows = {row["brand_id"]: row for row in T.brand_summary(enriched, brands).collect()}
    first = rows[1]
    assert (first["brand_name"], first["orders"], first["revenue"]) == ("Night Noodle", 1, 20.5)
    assert (rows[2]["brand_name"], rows[2]["orders"]) == ("Green Bowl", 1)
