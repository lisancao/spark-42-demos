"""Python UDFs need the same Python minor version on the client and the executors.

    python examples/06_python_versions.py
    uv run --no-project --python 3.12 --with pyspark-client==4.2.0 --with "pandas<3" \
        examples/06_python_versions.py

Blog post §7, "Python Version Coupling".
"""
import os
import sys

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(remote).getOrCreate()


@F.udf("string")
def executor_python(_):
    import sys

    return ".".join(map(str, sys.version_info[:3]))


client = ".".join(map(str, sys.version_info[:3]))
try:
    executor = spark.range(1).select(executor_python("id")).collect()[0][0]
    print(f"client Python {client}, executor Python {executor}")
except PySparkException as exc:
    lines = str(exc).splitlines()
    mismatch = [line[line.find("[") :] for line in lines if "PYTHON_VERSION_MISMATCH" in line]
    print(f"client Python {client}: {(mismatch or lines)[0]}")
spark.stop()

# Match the client's Python minor version to the Spark image's (3.10 for apache/spark:4.2.0).
