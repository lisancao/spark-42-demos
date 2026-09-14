"""A UDF that imports a local module fails until the module is shipped with addArtifact.

    python examples/08_add_artifact.py

Blog post §9, Layer 4.
"""
import os
from pathlib import Path

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GEO = Path(__file__).resolve().parent.parent / "pipeline" / "lib" / "geo.py"
remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(remote).create()  # a new session, with no artifacts yet


@F.udf("double")
def km_from_toronto(lat, lng):
    from geo import haversine_km  # imported on the executor, when the UDF runs

    return round(haversine_km(43.65, -79.38, lat, lng), 1)


cities = spark.createDataFrame(
    [("Montreal", 45.50, -73.57), ("Vancouver", 49.28, -123.12)],
    "city string, lat double, lng double",
)
with_distance = cities.withColumn("km_from_toronto", km_from_toronto("lat", "lng"))

try:
    with_distance.show()
except PySparkException as exc:
    lines = str(exc).splitlines()
    missing = [line.strip() for line in lines if "ModuleNotFoundError" in line]
    print(f"before addArtifact: {(missing or lines)[0]}")

spark.addArtifact(str(GEO), pyfile=True)
print("after addArtifact:")
with_distance.show()
spark.stop()

# Artifacts belong to the session, so ship them before the first action that needs them.
