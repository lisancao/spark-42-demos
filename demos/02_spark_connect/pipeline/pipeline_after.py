"""The orders pipeline on Spark Connect, run as an ordinary Python program.

    SPARK_REMOTE=sc://localhost:15002 \
        python pipeline/pipeline_after.py --output /opt/spark/work-dir/out/connect

pipeline_before.py is the Spark Classic version of this pipeline. MIGRATION.md compares them.
"""
import argparse
import os
from pathlib import Path

from lib import transforms as T
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve().parent
INPUT = os.environ.get("PIPELINE_INPUT", "/demo/pipeline/input")  # Layer 2: a path on the server

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
args = parser.parse_args()


def build_session(name: str) -> SparkSession:  # Layer 0
    remote = os.getenv("SPARK_REMOTE")
    builder = SparkSession.builder.appName(f"Pipeline_{name}")
    if remote:
        return builder.remote(remote).getOrCreate()
    if os.getenv("SPARK_API_MODE") == "connect":
        return builder.config("spark.api.mode", "connect").getOrCreate()
    return builder.getOrCreate()


spark = build_session("orders")
spark.conf.set("spark.sql.shuffle.partitions", "8")  # Layer 3: a value the server reads
spark.addArtifact(str(HERE / "lib" / "geo.py"), pyfile=True)  # Layer 4

events = T.parse_events(spark.read.json(f"{INPUT}/order_events.jsonl", schema=T.EVENT_SCHEMA))
if events.isEmpty():  # Layer 1
    raise SystemExit("no input events")
stores = spark.read.csv(f"{INPUT}/stores.csv", header=True, schema=T.STORE_SCHEMA)
brands = spark.read.csv(f"{INPUT}/brands.csv", header=True, schema=T.BRAND_SCHEMA)

enriched = T.enrich_orders(events, stores, T.distance_udf())
lifecycle = T.order_lifecycle(enriched)
outputs = {
    "bronze_events": events,
    "bronze_stores": stores,
    "bronze_brands": brands,
    "silver_orders_enriched": enriched,
    "silver_order_lifecycle": lifecycle,
    "gold_delivery_performance": T.delivery_performance(lifecycle, enriched),
    "gold_brand_summary": T.brand_summary(enriched, brands),
}
for name, df in outputs.items():
    path = f"{args.output}/{name}"
    df.write.mode("overwrite").parquet(path)
    print(f"{name:<28} {spark.read.parquet(path).count():>4} rows")
spark.stop()
