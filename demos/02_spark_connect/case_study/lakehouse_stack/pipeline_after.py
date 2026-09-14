#!/usr/bin/env python3
# ==============================================================================================
# Migrated pipeline, for Spark Connect or Spark Classic
# ==============================================================================================
# Derived from pipeline_before.py, which copies
# ~/lakehouse-stack/scripts/pipelines/pipeline_spark41.py. The transformations are byte-identical.
# Session creation, the catalog default, a preflight check and the startup message differ, and
# parity.py compares the tables that the two versions write.
#
# Run it either way:
#     SPARK_REMOTE=sc://localhost:15003 python pipeline_after.py     # Spark Connect
#     python pipeline_after.py                                        # Spark Classic
#
# Migration layers (blog post §9) and where each appears:
#     L0  session creation ............ build_session()
#     L1  pipeline logic .............. unchanged
#     L2  data sources and catalogs ... file paths and catalog names resolve on the server
#     L3  configuration ............... extensions and jars are set on the server; see preflight()
#     L5  execution and operations .... setLogLevel() removed; no docker exec
#     L6  security and identity ....... the client holds no catalog credentials
#
# L4 (dependencies) does not apply, because the pipeline has no UDFs.
# examples/08_add_artifact.py shows addArtifact().
# ==============================================================================================

#!/usr/bin/env python3
"""
Spark 4.1 Declarative Pipeline
==============================

Pipeline using decorator-based approach that mimics Spark Declarative Pipelines (SDP).
Functions define WHAT tables contain, not HOW to execute them.

Key differences from imperative:
  - Functions only RETURN DataFrames (no write statements)
  - Dependencies inferred from spark.table() calls
  - Execution order determined automatically
  - Single run() call executes everything

Usage:
    # On Spark 4.1 cluster
    docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/pipeline_spark41.py

Note: In production with actual SDP, you would use:
    spark-pipelines run --spec scripts/spark-pipeline.yml
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from functools import wraps
from typing import Callable, Dict, List, Set
import re


# =============================================================================
# SESSION (Classic or Connect, same file)
# =============================================================================
# PIPELINE_CATALOG changes where tables are written (Pipeline.run builds f"{catalog}.{table}"),
# but not where they are read: the silver and gold functions call spark.table("iceberg.bronze...")
# with the catalog name in the transformation body, which stays identical to pipeline_before.py.
# Setting PIPELINE_CATALOG alone would write new tables from the earlier run's bronze tables, and
# a parity check would then pass without comparing two independent runs.
#
# To keep two runs apart, leave the catalog as "iceberg" and point each run at a server whose
# `iceberg` catalog uses a different warehouse. README.md describes the setup.
DEFAULT_CATALOG = os.getenv("PIPELINE_CATALOG", "iceberg")


def build_session(name: str) -> SparkSession:
    """Create a Spark Connect session if SPARK_REMOTE is set, otherwise a Spark Classic session.

    SPARK_API_MODE=connect sets spark.api.mode instead (blog post §6). With neither variable
    set, the pipeline runs on Spark Classic as before, so a job can move back without code changes.
    """
    remote = os.getenv("SPARK_REMOTE")
    builder = SparkSession.builder.appName(f"Pipeline_{name}")
    if remote:
        return builder.remote(remote).getOrCreate()
    if os.getenv("SPARK_API_MODE") == "connect":
        return builder.config("spark.api.mode", "connect").getOrCreate()
    return builder.getOrCreate()


def preflight(spark, catalog: str) -> None:
    """Exit before any table is written if the server does not provide the pipeline's catalog.

    Under Spark Connect, catalogs, extensions and jars are configured on the server, so two servers
    that accept the same connection string can differ in what they provide.
    """
    root = catalog.split(".")[0]
    try:
        spark.sql(f"SHOW NAMESPACES IN {root}").count()
    except Exception as exc:
        raise SystemExit(
            f"preflight failed: catalog '{root}' is not configured on the Spark Connect server "
            f"at {os.getenv('SPARK_REMOTE', '<classic>')}.\n"
            f"  {type(exc).__name__}: {str(exc).splitlines()[0]}\n"
            "spark.sql.catalog.* and spark.sql.extensions name classes that the server's JVM "
            "loads, so set them where the server starts rather than with .config() in the client."
        ) from exc


# =============================================================================
# PIPELINE FRAMEWORK (Mimics SDP)
# =============================================================================

class Pipeline:
    """Mini SDP framework that demonstrates declarative pipeline patterns.

    In production, you'd use: from pyspark import pipelines as dp
    """

    def __init__(self, name: str, catalog: str = DEFAULT_CATALOG):
        self.name = name
        # Layers 2 and 3: the catalog is still named by a string, but spark.sql.catalog.iceberg
        # and spark.sql.extensions are now set on the Connect server. Their values are Java class
        # names, and the client has no JVM to load them. A client-side .config() for a catalog is
        # accepted and has no effect until the catalog is used.
        #
        # DEFAULT_CATALOG is "iceberg", the catalog name that production uses, because the
        # transformations read from spark.table("iceberg...") and a different name would move only
        # the writes. Runs are separated by warehouse instead, so running this against a
        # production Connect server writes to production tables.
        self.catalog = catalog
        self.tables: Dict[str, dict] = {}
        self._spark: SparkSession = None

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            # Layer 0: build_session() replaces SparkSession.builder, so the same file runs on
            # Spark Connect when SPARK_REMOTE is set and on Spark Classic when it is not.
            #
            # Layer 5: the original called self._spark.sparkContext.setLogLevel("WARN") here.
            # SparkContext is not available under Spark Connect, and the log level belongs to the
            # server's JVM, so it is set in the server's log4j2 configuration. The same call
            # appears 21 times in ~/lakehouse-stack/scripts; tools/compat_audit.py reports each.
            self._spark = build_session(self.name)
        return self._spark

    def materialized_view(self, name: str, layer: str = "bronze"):
        """Decorator that registers a table definition."""
        def decorator(func: Callable):
            # Infer dependencies from spark.table() calls
            import inspect
            source = inspect.getsource(func)
            deps = set(re.findall(r'spark\.table\(["\']([^"\']+)["\']\)', source))

            self.tables[name] = {
                'func': func,
                'layer': layer,
                'deps': deps,
            }

            @wraps(func)
            def wrapper():
                return func()
            return wrapper
        return decorator

    def _get_execution_order(self) -> List[str]:
        """Topologically sort tables based on dependencies."""
        visited: Set[str] = set()
        order: List[str] = []

        def visit(table_name: str):
            if table_name in visited:
                return
            visited.add(table_name)

            if table_name in self.tables:
                for dep in self.tables[table_name]['deps']:
                    short_dep = dep.replace(f"{self.catalog}.", "")
                    visit(short_dep)
            order.append(table_name)

        for table_name in self.tables:
            visit(table_name)

        return order

    def run(self, layer: str = None) -> Dict[str, int]:
        """Execute the pipeline."""
        results = {}
        preflight(self.spark, self.catalog)
        order = self._get_execution_order()

        mode = "CONNECT @ " + os.environ["SPARK_REMOTE"] if os.getenv("SPARK_REMOTE") else "CLASSIC"
        print(f"\nSpark Version: {self.spark.version}   Mode: {mode}   Catalog: {self.catalog}")
        print("=" * 60)
        print("DECLARATIVE PIPELINE (Spark 4.1 Style)")
        print("=" * 60)
        print(f"\nExecution order (auto-resolved): {order}")

        for table_name in order:
            if table_name not in self.tables:
                continue

            info = self.tables[table_name]
            if layer and info['layer'] != layer:
                continue

            full_name = f"{self.catalog}.{table_name}"
            deps = info['deps']

            layer_name = info['layer'].upper()
            print(f"\n[{layer_name}] {table_name}")
            if deps:
                print(f"  Dependencies: {deps}")

            # Execute and write
            df = info['func']()
            df.write.mode("overwrite").saveAsTable(full_name)

            # Get count from table (avoids re-scan of large DataFrames)
            count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {full_name}").collect()[0]['cnt']
            results[table_name] = count
            print(f"  -> {count:,} rows")

        print("\n" + "=" * 60)
        print(f"Pipeline complete: {len(results)} tables created")
        print("=" * 60)

        return results

    def stop(self):
        if self._spark:
            self._spark.stop()


# Create pipeline instance
pipeline = Pipeline("lakehouse_pipeline", catalog=DEFAULT_CATALOG)
spark = pipeline.spark  # Global spark for table functions

# Layer 1: the code below is byte-identical to pipeline_before.py. The framework infers
# dependencies with inspect.getsource() and a regular expression over each function body (see
# Pipeline.materialized_view). That runs in the client's Python and does not involve Spark.
#
# Layer 2: the bronze functions call spark.read.parquet("/data/..."). Under Spark Connect those
# paths resolve on the server, and they work here because the case study server mounts the data
# at /data. On a server without that mount, each bronze table fails with [PATH_NOT_FOUND].


# =============================================================================
# BRONZE LAYER - Raw Data Ingestion
# =============================================================================

@pipeline.materialized_view(name="bronze.dim_categories", layer="bronze")
def dim_categories():
    """Food categories dimension table."""
    return spark.read.parquet("/data/dimensions/categories.parquet")


@pipeline.materialized_view(name="bronze.dim_brands", layer="bronze")
def dim_brands():
    """Ghost kitchen brands dimension table."""
    return spark.read.parquet("/data/dimensions/brands.parquet")


@pipeline.materialized_view(name="bronze.dim_items", layer="bronze")
def dim_items():
    """Menu items dimension table."""
    return spark.read.parquet("/data/dimensions/items.parquet")


@pipeline.materialized_view(name="bronze.dim_locations", layer="bronze")
def dim_locations():
    """Delivery locations dimension table."""
    return spark.read.parquet("/data/dimensions/locations.parquet")


@pipeline.materialized_view(name="bronze.orders", layer="bronze")
def orders_batch():
    """Order lifecycle events with timestamp parsing."""
    df = spark.read.parquet("/data/events/orders_90d.parquet")
    return df.withColumn(
        "event_timestamp",
        f.to_timestamp(f.regexp_replace("ts", "T", " "))
    )


# =============================================================================
# SILVER LAYER - Cleaned and Enriched
# =============================================================================

@pipeline.materialized_view(name="silver.orders_enriched", layer="silver")
def orders_enriched():
    """Orders with parsed JSON body, time features, and location join.

    Dependencies auto-inferred from spark.table() calls below.
    """
    orders = spark.table("iceberg.bronze.orders")
    locations = spark.table("iceberg.bronze.dim_locations")

    # Filter nulls
    cleaned = orders.filter(
        f.col("event_id").isNotNull() &
        f.col("order_id").isNotNull() &
        f.col("event_timestamp").isNotNull()
    )

    # Parse JSON body
    body_schema = StructType([
        StructField("brand_id", IntegerType(), True),
        StructField("item_ids", StringType(), True),
        StructField("total", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("driver_id", StringType(), True),
    ])

    enriched = cleaned.withColumn("body_parsed", f.from_json("body", body_schema))

    # Extract fields
    enriched = enriched.select(
        "event_id", "event_type", "event_timestamp", "ts", "ts_seconds",
        "order_id", "location_id", "sequence", "body",
        f.col("body_parsed.brand_id").alias("brand_id"),
        f.col("body_parsed.total").alias("order_total"),
        f.col("body_parsed.lat").alias("latitude"),
        f.col("body_parsed.lng").alias("longitude"),
        f.col("body_parsed.driver_id").alias("driver_id"),
    )

    # Add time features
    enriched = enriched.withColumns({
        "event_hour": f.hour("event_timestamp"),
        "event_day_of_week": f.dayofweek("event_timestamp"),
        "is_weekend": f.when(f.dayofweek("event_timestamp").isin(1, 7), True).otherwise(False),
        "event_date": f.to_date("event_timestamp"),
    })

    # Join with locations
    locations_lookup = locations.select(
        f.col("id").alias("location_id"),
        f.col("city").alias("city_name"),
    )

    return enriched.join(f.broadcast(locations_lookup), on="location_id", how="left")


@pipeline.materialized_view(name="silver.order_lifecycle", layer="silver")
def order_lifecycle():
    """Pivoted view with one row per completed order and duration metrics."""
    orders = spark.table("iceberg.silver.orders_enriched")

    # Pivot events to columns
    lifecycle = orders.groupBy("order_id", "location_id", "city_name").pivot(
        "event_type",
        ["order_created", "kitchen_started", "kitchen_finished", "order_ready",
         "driver_arrived", "driver_picked_up", "delivered"]
    ).agg(f.min("event_timestamp").alias("ts"))

    # Rename columns
    lifecycle = lifecycle.select(
        "order_id", "location_id", "city_name",
        f.col("order_created").alias("created_at"),
        f.col("kitchen_started").alias("kitchen_started_at"),
        f.col("kitchen_finished").alias("kitchen_finished_at"),
        f.col("order_ready").alias("order_ready_at"),
        f.col("driver_arrived").alias("driver_arrived_at"),
        f.col("driver_picked_up").alias("pickup_at"),
        f.col("delivered").alias("delivered_at"),
    )

    # Calculate durations
    lifecycle = lifecycle.withColumns({
        "kitchen_duration_min": (f.unix_timestamp("kitchen_finished_at") - f.unix_timestamp("kitchen_started_at")) / 60,
        "delivery_duration_min": (f.unix_timestamp("delivered_at") - f.unix_timestamp("pickup_at")) / 60,
        "total_duration_min": (f.unix_timestamp("delivered_at") - f.unix_timestamp("created_at")) / 60,
    })

    # Filter to completed orders
    return lifecycle.filter(f.col("delivered_at").isNotNull())


# =============================================================================
# GOLD LAYER - Business Aggregations
# =============================================================================

@pipeline.materialized_view(name="gold.hourly_metrics", layer="gold")
def hourly_metrics():
    """Hourly order metrics by location."""
    orders = spark.table("iceberg.silver.orders_enriched")

    return orders.filter(f.col("event_type") == "order_created").groupBy(
        "event_date", "event_hour", "location_id", "city_name"
    ).agg(
        f.count("order_id").alias("order_count"),
        f.sum("order_total").alias("total_revenue"),
        f.avg("order_total").alias("avg_order_value"),
        f.countDistinct("brand_id").alias("unique_brands"),
    )


@pipeline.materialized_view(name="gold.delivery_performance", layer="gold")
def delivery_performance():
    """Delivery performance metrics by date and location."""
    lifecycle = spark.table("iceberg.silver.order_lifecycle")

    return lifecycle.groupBy(
        f.to_date("created_at").alias("order_date"),
        "location_id", "city_name"
    ).agg(
        f.count("order_id").alias("completed_orders"),
        f.avg("kitchen_duration_min").alias("avg_kitchen_time_min"),
        f.avg("delivery_duration_min").alias("avg_delivery_time_min"),
        f.avg("total_duration_min").alias("avg_total_time_min"),
        f.percentile_approx("total_duration_min", 0.5).alias("median_total_time_min"),
        f.percentile_approx("total_duration_min", 0.95).alias("p95_total_time_min"),
    )


@pipeline.materialized_view(name="gold.brand_summary", layer="gold")
def brand_summary():
    """Brand-level summary metrics."""
    orders = spark.table("iceberg.silver.orders_enriched")
    brands = spark.table("iceberg.bronze.dim_brands")

    brand_metrics = orders.filter(f.col("event_type") == "order_created").groupBy("brand_id").agg(
        f.count("order_id").alias("total_orders"),
        f.sum("order_total").alias("total_revenue"),
        f.avg("order_total").alias("avg_order_value"),
        f.countDistinct("location_id").alias("locations_served"),
        f.min("event_date").alias("first_order_date"),
        f.max("event_date").alias("last_order_date"),
    )

    return brand_metrics.join(
        brands.select(f.col("id").alias("brand_id"), "name"),
        on="brand_id", how="left"
    ).select(
        "brand_id", f.col("name").alias("brand_name"),
        "total_orders", "total_revenue", "avg_order_value",
        "locations_served", "first_order_date", "last_order_date",
    )


# =============================================================================
# PIPELINE EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Single call runs everything in correct order
    results = pipeline.run()
    pipeline.stop()
