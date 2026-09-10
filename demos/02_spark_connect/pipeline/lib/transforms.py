"""DataFrame transformations for the orders pipeline. No I/O and no session handling.

pipeline_before.py and pipeline_after.py call these functions unchanged, and tests/spark runs them
on Spark Classic and on Spark Connect.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

EVENT_SCHEMA = (
    "event_id string, event_type string, ts string, order_id string, store_id int, body string"
)
STORE_SCHEMA = "id int, city string, lat double, lng double"
BRAND_SCHEMA = "id int, name string"
BODY_SCHEMA = "brand_id int, total double, lat double, lng double"
EVENT_TYPES: list = ["order_created", "order_ready", "driver_picked_up", "delivered"]


def parse_events(raw: DataFrame) -> DataFrame:
    """Add event_timestamp, parsed from the ISO 8601 ts string."""
    return raw.withColumn("event_timestamp", F.to_timestamp(F.regexp_replace("ts", "T", " ")))


def distance_udf():
    """Return a UDF that computes the distance from the store to the delivery point, in km.

    The UDF imports the geo module on the executor, so geo.py must be shipped with the job.
    """

    @F.udf("double")
    def delivery_km(store_lat, store_lng, lat, lng):
        from geo import haversine_km

        if None in (store_lat, store_lng, lat, lng):
            return None
        return round(haversine_km(store_lat, store_lng, lat, lng), 3)

    return delivery_km


def enrich_orders(events: DataFrame, stores: DataFrame, delivery_km) -> DataFrame:
    """Drop incomplete events, parse the JSON body, and add time, store and distance columns."""
    parsed = events.filter(
        F.col("event_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
    ).withColumn("body_parsed", F.from_json("body", BODY_SCHEMA))

    enriched = parsed.select(
        "event_id",
        "event_type",
        "event_timestamp",
        "order_id",
        "store_id",
        F.col("body_parsed.brand_id").alias("brand_id"),
        F.col("body_parsed.total").alias("order_total"),
        F.col("body_parsed.lat").alias("latitude"),
        F.col("body_parsed.lng").alias("longitude"),
    ).withColumns(
        {
            "event_hour": F.hour("event_timestamp"),
            "is_weekend": F.dayofweek("event_timestamp").isin(1, 7),
            "event_date": F.to_date("event_timestamp"),
        }
    )

    store_lookup = stores.select(
        F.col("id").alias("store_id"),
        "city",
        F.col("lat").alias("store_lat"),
        F.col("lng").alias("store_lng"),
    )
    return (
        enriched.join(F.broadcast(store_lookup), on="store_id", how="left")
        .withColumn("delivery_km", delivery_km("store_lat", "store_lng", "latitude", "longitude"))
        .drop("store_lat", "store_lng")
    )


def _minutes(end: str, start: str) -> Column:
    return (F.unix_timestamp(end) - F.unix_timestamp(start)) / 60


def order_lifecycle(enriched: DataFrame) -> DataFrame:
    """Return one row per delivered order, with preparation and delivery times in minutes."""
    pivoted = (
        enriched.groupBy("order_id", "store_id", "city")
        .pivot("event_type", EVENT_TYPES)
        .agg(F.min("event_timestamp"))
    )
    lifecycle = pivoted.select(
        "order_id",
        "store_id",
        "city",
        F.col("order_created").alias("created_at"),
        F.col("order_ready").alias("ready_at"),
        F.col("driver_picked_up").alias("picked_up_at"),
        F.col("delivered").alias("delivered_at"),
    ).withColumns(
        {
            "prep_min": _minutes("ready_at", "created_at"),
            "delivery_min": _minutes("delivered_at", "picked_up_at"),
            "total_min": _minutes("delivered_at", "created_at"),
        }
    )
    return lifecycle.filter(F.col("delivered_at").isNotNull())


def delivery_performance(lifecycle: DataFrame, enriched: DataFrame) -> DataFrame:
    """Summarize delivered orders by city."""
    distance = enriched.filter(F.col("event_type") == "delivered").select("order_id", "delivery_km")
    return (
        lifecycle.join(distance, on="order_id", how="left")
        .groupBy("city")
        .agg(
            F.count("order_id").alias("delivered_orders"),
            F.round(F.avg("prep_min"), 2).alias("avg_prep_min"),
            F.round(F.avg("total_min"), 2).alias("avg_total_min"),
            F.percentile_approx("total_min", 0.5).alias("median_total_min"),
            F.percentile_approx("total_min", 0.95).alias("p95_total_min"),
            F.round(F.avg("delivery_km"), 3).alias("avg_delivery_km"),
        )
    )


def brand_summary(enriched: DataFrame, brands: DataFrame) -> DataFrame:
    """Summarize created orders by brand."""
    metrics = (
        enriched.filter(F.col("event_type") == "order_created")
        .groupBy("brand_id")
        .agg(
            F.count("order_id").alias("orders"),
            F.round(F.sum("order_total"), 2).alias("revenue"),
            F.count_distinct("store_id").alias("stores"),
            F.min("event_date").alias("first_order_date"),
            F.max("event_date").alias("last_order_date"),
        )
    )
    names = brands.select(F.col("id").alias("brand_id"), F.col("name").alias("brand_name"))
    return metrics.join(names, on="brand_id", how="left").select(
        "brand_id",
        "brand_name",
        "orders",
        "revenue",
        "stores",
        "first_order_date",
        "last_order_date",
    )
