"""Check a Spark Connect setup: the client, the endpoint, the server, and its capabilities.

    python tools/doctor.py --remote sc://localhost:15002 --expect-version 4.2

Exit status is 0 when every check passes, 1 when a check fails, and 2 when the server cannot be
reached.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import os
import re
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

GEO = Path(__file__).resolve().parent.parent / "pipeline" / "lib" / "geo.py"
CONFIG_KEYS = (
    "spark.api.mode",
    "spark.sql.execution.arrow.pyspark.enabled",
    "spark.sql.execution.pythonUDF.arrow.enabled",
    "spark.sql.shuffle.partitions",
)
SECRET_HINTS = ("password", "secret", "token", "credential", "access.key")


def report(status: str, name: str, detail: object = "") -> None:
    print(f"  {status:<4} {name:<22} {detail}".rstrip())


def first_line(exc: BaseException) -> str:
    lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    # A Python worker failure states its cause on the last exception line of the traceback.
    causes = [
        re.sub(r"^[\w.]*\.(?=\w+: )", "", line)
        for line in lines
        if re.match(r"^[\w.]+(Error|Exception): ", line)
    ]
    tagged = [line for line in lines if line.startswith("[") or "Error:" in line]
    return (causes[-1:] or tagged or lines or [type(exc).__name__])[0][:110]


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
    report("", "py4j", "present" if importlib.util.find_spec("py4j") else "absent")
    report("", "python", ".".join(map(str, sys.version_info[:3])))


def reachable(remote: str) -> bool:
    parsed = urlparse(remote.split(";")[0])
    with socket.socket() as probe:
        probe.settimeout(3)
        return probe.connect_ex((parsed.hostname or "localhost", parsed.port or 15002)) == 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Check a Spark Connect setup.")
    parser.add_argument("--remote", default=os.environ.get("SPARK_REMOTE", "sc://localhost:15002"))
    parser.add_argument("--expect-version", default="4.2")
    args = parser.parse_args()

    describe_client()
    print("endpoint")
    report("", "url", args.remote)
    if not reachable(args.remote):
        report("FAIL", "reachable", "nothing is listening at this address")
        return 2
    report("ok", "reachable")

    from pyspark.sql import Observation, SparkSession
    from pyspark.sql import functions as F

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

    @F.udf("string")
    def executor_python(_):
        import sys

        return ".".join(map(str, sys.version_info[:2]))

    def observe_rows() -> int:
        observation = Observation("rows")
        spark.range(10).observe(observation, F.count(F.lit(1)).alias("n")).collect()
        return observation.get["n"]

    def python_versions_match() -> str:
        executor = spark.range(1).select(executor_python("id")).collect()[0][0]
        client = ".".join(map(str, sys.version_info[:2]))
        if executor != client:
            raise RuntimeError(f"client Python {client}, executor Python {executor}")
        return f"both {client}"

    def add_artifact() -> str:
        session = SparkSession.builder.remote(args.remote).create()

        @F.udf("double")
        def km(lat):
            from geo import haversine_km

            return haversine_km(0.0, 0.0, lat, 0.0)

        query = session.range(1).select(km(F.lit(1.0)))
        try:
            query.collect()
            raise RuntimeError("the module was importable before it was shipped")
        except Exception as exc:
            if "No module named 'geo'" not in str(exc):
                raise
        session.addArtifact(str(GEO), pyfile=True)
        query.collect()
        session.stop()
        return "fails before addArtifact, succeeds after"

    print("capabilities")
    check("sql", lambda: spark.sql("SELECT 1 AS one").collect()[0][0])
    check("arrow collect", lambda: f"{len(spark.range(1000).toPandas())} rows")
    check("observe", observe_rows)
    check("python versions", python_versions_match)
    check("add artifact", add_artifact)

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
