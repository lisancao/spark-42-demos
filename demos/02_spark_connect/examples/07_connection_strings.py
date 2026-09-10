"""A "/" inside a connection string parameter value is reported as a non-empty path.

    python examples/07_connection_strings.py

Companion guide §5, "Constraints".
"""
import os

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002").rstrip("/")

for user_agent in ("myapp/setup", "myapp-setup"):
    url = f"{remote}/;user_agent={user_agent}"
    try:
        spark = SparkSession.builder.remote(url).create()
        print(f"accepted  {url}  (Spark {spark.version})")
        spark.stop()
    except PySparkException as exc:
        print(f"rejected  {url}")
        print(f"          {str(exc).splitlines()[0]}")

# The error names the path, not the parameter, so validate values before building the string.
