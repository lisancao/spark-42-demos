"""Run the same Spark Connect operations for one client and server pairing, and record the results.

    python extras/version_matrix/matrix.py --cell B --run 1 --remote sc://localhost:15096
    python extras/version_matrix/matrix.py --report

Results are written to extras/version_matrix/results/<cell>-run<N>.json and summarized in blog
post §2, Table 2-1. The operations are a sample, not a compatibility matrix.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
GEO = HERE.parents[1] / "pipeline" / "lib" / "geo.py"
CELLS = {
    "A": "client 4.2.0, server 4.2.0",
    "B": "client 4.2.0, server 4.1.2",
    "C": "client 4.1.2, server 4.2.0",
    "D": "client 4.1.2, server 4.1.2",
}


def first_line(exc: BaseException) -> str:
    lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    grpc_status = [line for line in lines if "StatusCode." in line]
    tagged = [line[line.find("[") :] for line in lines if "[" in line and "]" in line]
    if not lines:
        return f"{type(exc).__name__} (no message)"
    return f"{type(exc).__name__}: {(grpc_status or tagged or lines)[0][:150]}"


def run(remote: str) -> dict:
    import pyspark
    from pyspark.sql import Observation, SparkSession
    from pyspark.sql import functions as F

    ops: dict[str, dict] = {}

    def op(name, fn, available=True):
        if not available:
            ops[name] = {"status": "n/a", "detail": "not in this client version"}
            return
        try:
            ops[name] = {"status": "ok", "detail": str(fn())}
        except Exception as exc:
            ops[name] = {"status": "fails", "detail": first_line(exc)}

    spark = SparkSession.builder.remote(remote).create()

    @F.udf("string")
    def executor_python(_):
        import sys

        return ".".join(map(str, sys.version_info[:3]))

    def observe():
        observation = Observation("rows")
        spark.range(25).observe(observation, F.count(F.lit(1)).alias("n")).collect()
        return observation.get["n"]

    def add_artifact():
        session = SparkSession.builder.remote(remote).create()

        @F.udf("double")
        def km_north(lat):
            from geo import haversine_km

            return haversine_km(0.0, 0.0, lat, 0.0)

        query = session.range(1).select(km_north(F.lit(1.0)))
        try:
            query.collect()
            return "the module was importable before it was shipped"
        except Exception as exc:
            if "No module named 'geo'" not in str(exc):
                raise
        session.addArtifact(str(GEO), pyfile=True)
        km = round(query.collect()[0][0], 1)
        session.stop()
        return f"fails before addArtifact, {km} km after"

    def analysis_error():
        try:
            spark.range(1).select("no_such_column").collect()
        except Exception as exc:
            state = getattr(exc, "getSqlState", lambda: None)()
            return f"{first_line(exc).split(']')[0]}], SQL state {state}"
        raise RuntimeError("no error was raised")

    frame = spark.range(3)
    op("connect", lambda: spark.version)
    op("sql", lambda: spark.sql("SELECT 1 AS one").collect()[0][0])
    op("createDataFrame (local data)", lambda: spark.createDataFrame(
        [("a", 1), ("b", 2), ("a", 3)], "k string, v int").count())
    op("group by", lambda: spark.range(10).groupBy((F.col("id") % 2).alias("k"))
       .agg(F.sum("id").alias("s")).orderBy("k").collect())
    op("toPandas", lambda: len(spark.range(100_000).toPandas()))
    op("observe", observe)
    op("python udf", lambda: spark.range(1).select(executor_python("id")).collect()[0][0])
    op("addArtifact", add_artifact)
    op("zipWithIndex", lambda: frame.zipWithIndex().count(), hasattr(type(frame), "zipWithIndex"))
    # The input comes from SQL so that this row does not depend on createDataFrame.
    op("read.json(DataFrame)", lambda: spark.read.json(
        spark.sql("""SELECT '{"a": 1}' AS value""")).count())
    op("emptyDataFrame(schema)", lambda: spark.emptyDataFrame("a int").count(),
       hasattr(type(spark), "emptyDataFrame"))
    op("GetStatus (private API)",
       lambda: f"{type(spark.client._get_operation_statuses()).__name__} returned",
       hasattr(spark.client, "_get_operation_statuses"))
    op("analysis error", analysis_error)
    op("interruptAll", lambda: f"{len(spark.interruptAll())} operations interrupted")

    environment = {
        "client_pyspark": pyspark.__version__,
        "client_python": ".".join(map(str, sys.version_info[:3])),
        "server_version": ops["connect"]["detail"],
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    spark.stop()
    return {"environment": environment, "operations": ops}


def report() -> None:
    runs = {path.stem: json.loads(path.read_text()) for path in sorted(RESULTS.glob("*.json"))}
    cells = [cell for cell in CELLS if f"{cell}-run1" in runs]
    names = list(runs[f"{cells[0]}-run1"]["operations"])
    print("| Operation | " + " | ".join(f"{c} ({CELLS[c]})" for c in cells) + " |")
    print("|---|" + "---|" * len(cells))
    for name in names:
        row = []
        for cell in cells:
            result = runs[f"{cell}-run1"]["operations"][name]
            text = result["status"] if result["status"] != "fails" else f"fails: {result['detail']}"
            if f"{cell}-run2" in runs and runs[f"{cell}-run2"]["operations"][name] != result:
                text += " (differs in run 2)"
            row.append(text)
        print(f"| {name} | " + " | ".join(row) + " |")


def main() -> int:
    parser = argparse.ArgumentParser(description="Record Spark Connect results for one pairing.")
    parser.add_argument("--cell", choices=sorted(CELLS))
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument("--remote")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    if args.report:
        report()
        return 0
    result = run(args.remote)
    result["cell"] = {"id": args.cell, "description": CELLS[args.cell], "remote": args.remote}
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / f"{args.cell}-run{args.run}.json").write_text(json.dumps(result, indent=2) + "\n")
    failed = [name for name, value in result["operations"].items() if value["status"] == "fails"]
    print(f"cell {args.cell} run {args.run}: {len(result['operations']) - len(failed)} ok or n/a, "
          f"{len(failed)} failed {failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
