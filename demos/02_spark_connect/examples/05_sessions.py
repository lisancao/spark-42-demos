"""getOrCreate() can return an existing session for a different URL; create() starts a new one.

    python examples/05_sessions.py

Blog post §7, "Session Reuse with getOrCreate()".
"""
import os

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")

# 1. getOrCreate() returns the active session, even when the builder names another server.
a = SparkSession.builder.remote(remote).getOrCreate()
b = SparkSession.builder.remote("sc://127.0.0.1:1").getOrCreate()  # nothing listens on port 1
print(f"getOrCreate() with a different URL returned the same session: {b is a}")

# 2. create() starts a separate server-side session each time.
c = SparkSession.builder.remote(remote).create()
d = SparkSession.builder.remote(remote).create()
c.range(3).createOrReplaceTempView("t")
d.range(7).createOrReplaceTempView("t")
print(f"temporary view t: {c.table('t').count()} rows in c, {d.table('t').count()} rows in d")
d.stop()
print(f"after d.stop(), c still reads {c.table('t').count()} rows")

# 3. create() does not accept a local connection string.
try:
    SparkSession.builder.remote("local[2]").create()
except PySparkException as exc:
    print(f"create() with local[2]: {str(exc).splitlines()[0]}")

c.stop()
a.stop()

# Use create() when one process talks to several servers or needs isolated sessions.
