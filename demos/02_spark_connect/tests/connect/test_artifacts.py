"""A Python UDF's dependency reaches the executors through addArtifact (guide §9, Layer 4)."""
import os
from pathlib import Path

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GEO = Path(__file__).resolve().parents[2] / "pipeline" / "lib" / "geo.py"
REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")


def test_udf_fails_until_its_module_is_shipped():
    session = SparkSession.builder.remote(REMOTE).create()
    try:

        @F.udf("double")
        def km_north(lat):
            from geo import haversine_km

            return haversine_km(0.0, 0.0, lat, 0.0)

        query = session.range(1).select(km_north(F.lit(1.0)).alias("km"))
        with pytest.raises(PySparkException, match="No module named 'geo'"):
            query.collect()

        session.addArtifact(str(GEO), pyfile=True)
        assert round(query.collect()[0]["km"], 1) == 111.2
    finally:
        session.stop()
