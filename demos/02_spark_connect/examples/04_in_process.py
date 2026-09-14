"""Start a Spark Connect server inside this Python process with remote("local[*]").

    python examples/04_in_process.py    # full pyspark (.venv-full) and a JDK

Blog post §4 and setup/TOPOLOGIES.md §1.
"""
import socket
import sys

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

# In local mode the client connects to localhost:15002, whatever binding.port is set to.
LOCAL_PORT = 15002

with socket.socket() as probe:
    if probe.connect_ex(("127.0.0.1", LOCAL_PORT)) == 0:
        print(f"skipped: port {LOCAL_PORT} is in use.")
        print("In local mode the client always connects to localhost:15002, so the session")
        print("would reach whichever server holds that port. Stop that server, or connect")
        print("to it with an sc:// URL.")
        sys.exit(0)

try:
    builder = SparkSession.builder.remote("local[*]").config("spark.ui.enabled", "false")
    spark = builder.getOrCreate()
except PySparkException as exc:
    print(f"cannot start a local server: {str(exc).splitlines()[0]}")
    sys.exit(1)

print(f"Spark {spark.version}, session class from {type(spark).__module__}")

try:
    _ = spark.sparkContext
except PySparkException as exc:
    print(f"spark.sparkContext: {str(exc).splitlines()[0]}")

print("sum of 0..999 =", spark.range(1000).selectExpr("sum(id)").collect()[0][0])
spark.stop()

# This is the Connect code path with no separate server, which suits compatibility tests in CI.
