"""The orders pipeline on Spark Classic, submitted with spark-submit.

    spark-submit pipeline/pipeline_before.py --output /opt/spark/work-dir/out/classic

pipeline_after.py is the same pipeline migrated to Spark Connect. MIGRATION.md compares them.
"""
import argparse
from pathlib import Path

from lib import transforms as T
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve().parent
INPUT = str(HERE / "input")  # Layer 2: a path on the host that runs the driver

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
args = parser.parse_args()

spark = (
    SparkSession.builder.appName("orders-pipeline")  # Layer 0
    .config("spark.sql.shuffle.partitions", "8")  # Layer 3
    .enableHiveSupport()  # Layer 3
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # Layer 5
spark.sparkContext.addPyFile(str(HERE / "lib" / "geo.py"))  # Layer 4

events = T.parse_events(spark.read.json(f"{INPUT}/order_events.jsonl", schema=T.EVENT_SCHEMA))
if events.rdd.isEmpty():  # Layer 1
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
