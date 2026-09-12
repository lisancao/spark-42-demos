"""Check the metric views setup: the client, the server, the mounted dataset and a metric view.

    python tools/doctor.py --remote sc://localhost:15002 --expect-version 4.2

Exit status is 0 when every check passes, 1 when a check fails, and 2 when the server cannot be
reached.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

EXPECTED_SESSIONS = 79_000
CONFIG_KEYS = (
    "spark.sql.catalogImplementation",
    "spark.sql.warehouse.dir",
    "spark.sql.session.timeZone",
)
SECRET_HINTS = ("password", "secret", "token", "credential", "access.key")
METRIC_VIEW_YAML = """version: 0.1
source: doctor_sessions
dimensions:
  - name: region
    expr: region
measures:
  - name: sessions
    expr: COUNT(1)
"""


def report(status: str, name: str, detail: object = "") -> None:
    print(f"  {status:<4} {name:<22} {detail}".rstrip())


def first_line(exc: BaseException) -> str:
    lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    tagged = [line for line in lines if line.startswith("[") or "Error:" in line]
    return (tagged or lines or [type(exc).__name__])[0][:110]


def describe_client() -> None:
    print("client")
    for distribution in ("pyspark-client", "pyspark"):
        try:
            report("", "distribution", f"{distribution} {importlib.metadata.version(distribution)}")
            break
        except importlib.metadata.PackageNotFoundError:
            continue
    spec = importlib.util.find_spec("pyspark")
    jars = Path(spec.origin).parent / "jars" if spec and spec.origin else None
    report("", "jars", "present" if jars and jars.is_dir() else "none")
    report("", "python", ".".join(map(str, sys.version_info[:3])))


def reachable(remote: str) -> bool:
    parsed = urlparse(remote.split(";")[0])
    with socket.socket() as probe:
        probe.settimeout(3)
        return probe.connect_ex((parsed.hostname or "localhost", parsed.port or 15002)) == 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Check the metric views setup.")
    parser.add_argument("--remote", default=os.environ.get("SPARK_REMOTE", "sc://localhost:15002"))
    parser.add_argument("--expect-version", default="4.2")
    parser.add_argument(
        "--data",
        default=os.environ.get("DEMO_DATA", "/demo/data"),
        help="the dataset directory as the server sees it",
    )
    args = parser.parse_args()
    sessions_path = f"{args.data}/sessions.parquet"

    describe_client()
    print("endpoint")
    report("", "url", args.remote)
    if not reachable(args.remote):
        report("FAIL", "reachable", "nothing is listening at this address")
        return 2
    report("ok", "reachable")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(args.remote).create()
    results: list[bool] = []

    def check(name: str, probe) -> None:
        try:
            report("ok", name, probe())
            results.append(True)
        except Exception as exc:  # report every failure and keep checking
            report("FAIL", name, first_line(exc))
            results.append(False)

    print("server")
    version_ok = spark.version.startswith(args.expect_version)
    report("ok" if version_ok else "FAIL", "version", spark.version)
    results.append(version_ok)

    def dataset() -> str:
        rows = spark.read.parquet(sessions_path).count()
        if rows != EXPECTED_SESSIONS:
            raise RuntimeError(f"{rows:,} rows in {sessions_path}; expected {EXPECTED_SESSIONS:,}")
        return f"{rows:,} rows in {sessions_path}"

    def metric_view() -> str:
        # A metric view needs a persistent source, so the check registers a table of its own and
        # removes both objects afterward. Dropping an external table leaves its files in place.
        spark.sql("DROP VIEW IF EXISTS doctor_metrics")
        spark.sql("DROP TABLE IF EXISTS doctor_sessions")
        try:
            spark.sql(f"CREATE TABLE doctor_sessions USING parquet LOCATION '{sessions_path}'")
            spark.sql(
                f"CREATE VIEW doctor_metrics WITH METRICS LANGUAGE YAML AS $${METRIC_VIEW_YAML}$$"
            )
            total = spark.sql("SELECT MEASURE(sessions) FROM doctor_metrics").collect()[0][0]
        finally:
            spark.sql("DROP VIEW IF EXISTS doctor_metrics")
            spark.sql("DROP TABLE IF EXISTS doctor_sessions")
        if total != EXPECTED_SESSIONS:
            raise RuntimeError(f"MEASURE(sessions) returned {total}; expected {EXPECTED_SESSIONS}")
        return "created, queried with MEASURE() and dropped"

    print("dataset and metric views")
    check("dataset", dataset)
    check("metric view", metric_view)

    print("server configuration (values of secret-like keys are masked)")
    for key in CONFIG_KEYS:
        try:
            value = spark.conf.get(key)
        except Exception:  # an unset key raises SQL_CONF_NOT_FOUND
            value = "<unset>"
        masked = any(hint in key.lower() for hint in SECRET_HINTS)
        print(f"       {key:<46} {'****' if masked else value}")

    spark.stop()
    ok = all(results)
    print("all checks passed" if ok else "some checks failed")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
