"""Shared test configuration.

The tests are in three groups:

- tests/unit needs no Spark.
- tests/spark runs on Spark Classic when SPARK_REMOTE is unset (full pyspark and a JDK), and on
  Spark Connect when SPARK_REMOTE is set.
- tests/connect needs a reachable Spark Connect server at SPARK_REMOTE.
"""
import importlib.util
import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

import pytest

GEO = Path(__file__).resolve().parent.parent / "pipeline" / "lib" / "geo.py"


def _reachable(remote: str) -> bool:
    parsed = urlparse(remote.split(";")[0])
    with socket.socket() as probe:
        probe.settimeout(2)
        return probe.connect_ex((parsed.hostname or "localhost", parsed.port or 15002)) == 0


def pytest_collection_modifyitems(config, items):
    remote = os.environ.get("SPARK_REMOTE")
    server_up = bool(remote) and _reachable(remote)
    has_jvm_client = importlib.util.find_spec("py4j") is not None
    unavailable = f"no Spark Connect server at {remote}" if remote else "SPARK_REMOTE is not set"
    for item in items:
        group = item.path.parent.name
        if group == "connect" and not server_up:
            item.add_marker(pytest.mark.skip(reason=unavailable))
        elif group == "spark" and remote and not server_up:
            item.add_marker(pytest.mark.skip(reason=unavailable))
        elif group == "spark" and not remote and not has_jvm_client:
            item.add_marker(pytest.mark.skip(reason="pyspark-client needs SPARK_REMOTE"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    remote = os.environ.get("SPARK_REMOTE")
    if remote:
        session = SparkSession.builder.remote(remote).create()
    else:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        session = (
            SparkSession.builder.master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    session.addArtifact(str(GEO), pyfile=True)
    yield session
    session.stop()
