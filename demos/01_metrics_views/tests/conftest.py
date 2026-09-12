"""Shared test configuration.

The tests are in three groups:

- tests/unit needs no Spark.
- tests/spark runs on Spark Classic when SPARK_REMOTE is unset (full pyspark and a JDK), and on
  Spark Connect when SPARK_REMOTE is set.
- tests/connect needs a reachable Spark Connect server at SPARK_REMOTE.
"""
import importlib.util
import os
import re
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _reachable(remote: str) -> bool:
    parsed = urlparse(remote.split(";")[0])
    with socket.socket() as probe:
        probe.settimeout(2)
        return probe.connect_ex((parsed.hostname or "localhost", parsed.port or 15002)) == 0


def pytest_collection_modifyitems(items):
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
def data_dir() -> str:
    """The dataset directory as the Spark session sees it."""
    if os.environ.get("SPARK_REMOTE"):
        return os.environ.get("DEMO_DATA", "/demo/data")
    if not (ROOT / "data" / "sessions.parquet").exists():
        pytest.skip("data/ has no dataset; run make data")
    return str(ROOT / "data")


@pytest.fixture(scope="session")
def spark(data_dir, tmp_path_factory):
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
            .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
            .getOrCreate()
        )
    for table in ("sessions", "orders", "users"):
        session.sql(
            f"CREATE TABLE IF NOT EXISTS {table} USING parquet"
            f" LOCATION '{data_dir}/{table}.parquet'"
        )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def definition():
    """Return the DEFINITION string of an example, so the tests use the YAML the examples run."""

    def read(example: str) -> str:
        text = (ROOT / "examples" / example).read_text(encoding="utf-8")
        match = re.search(r'^DEFINITION = """(.*?)"""', text, re.S | re.M)
        assert match, f"examples/{example} has no DEFINITION"
        return match.group(1)

    return read
