#!/usr/bin/env python3
"""Measure the overhead of the Spark Connect protocol against Spark Classic on one machine.

    python extras/bench/protocol_overhead.py --mode classic --out classic.json   # full pyspark, JDK
    python extras/bench/protocol_overhead.py --mode connect --remote sc://localhost:15002 \\
        --out connect.json
    python extras/bench/protocol_overhead.py --compare classic.json connect.json

Scope, method and results: VERIFIED_FACTS.md §3 (single node, loopback, one client).
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

# A newly started JVM interprets a query until the JIT compiles it, while a long-running Connect
# server has compiled it already. Discarding warmup iterations keeps that out of the comparison.
WARMUP = 50
TRIALS = 20


@dataclass
class Measurement:
    name: str
    unit: str
    samples: list[float] = field(default_factory=list)
    note: str = ""

    @property
    def median(self) -> float | None:
        return statistics.median(self.samples) if self.samples else None

    @property
    def p95(self) -> float | None:
        if not self.samples:
            return None
        if len(self.samples) < 3:
            return max(self.samples)
        return sorted(self.samples)[max(0, round(0.95 * len(self.samples)) - 1)]


@dataclass
class Run:
    mode: str
    spark_version: str
    python: str
    host: str
    cores: int
    measurements: list[Measurement] = field(default_factory=list)

    def add(self, m: Measurement) -> None:
        self.measurements.append(m)
        med = m.median
        shown = f"{med:,.1f} {m.unit}" if med is not None else "n/a"
        print(f"  {m.name:<22} {shown:>16}" + (f"   {m.note}" if m.note else ""))


def timed(fn, warmup: int = WARMUP, trials: int = TRIALS) -> list[float]:
    """Run fn, discard warmup iterations, return per-trial milliseconds."""
    for _ in range(warmup):
        fn()
    out = []
    for _ in range(trials):
        t0 = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t0) * 1000.0)
    return out


def build_session(mode: str, remote: str | None):
    """Return (session, startup_ms). Startup is a single observation by construction."""
    from pyspark.sql import SparkSession

    t0 = time.perf_counter()
    if mode == "connect":
        if not remote:
            raise SystemExit("--remote is required for --mode connect")
        spark = SparkSession.builder.appName("bench-connect").remote(remote).getOrCreate()
    else:
        spark = (
            SparkSession.builder.appName("bench-classic")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    _ = spark.version  # force a real round trip / full init before stopping the clock
    return spark, (time.perf_counter() - t0) * 1000.0


def run_bench(mode: str, remote: str | None, warmup: int = WARMUP,
              trials: int = TRIALS) -> Run:
    from pyspark.sql import functions as F

    spark, startup_ms = build_session(mode, remote)

    run = Run(
        mode=mode,
        spark_version=spark.version,
        python=platform.python_version(),
        host=platform.node(),
        cores=os.cpu_count() or 0,
    )

    print(f"\nmode={mode}  server={spark.version}  python={run.python}  cores={run.cores}"
          f"  warmup={warmup} trials={trials}")
    print(f"  {'measurement':<22} {'median':>16}")
    print("  " + "-" * 56)

    run.add(Measurement("session_start", "ms", [startup_ms],
                        note="single observation (once per process)"))

    run.add(Measurement("trivial_query", "ms",
                        timed(lambda: spark.range(1).collect(), warmup, trials),
                        note="round-trip dominated"))

    run.add(Measurement("sql_select_1", "ms",
                        timed(lambda: spark.sql("SELECT 1").collect())))

    # A fresh DataFrame each time: schema caches after first access, so reusing one would measure
    # a dict lookup rather than an AnalyzePlan RPC.
    run.add(Measurement("schema_access", "ms",
                        timed(lambda: spark.range(10).select("id").schema, warmup, trials),
                        note="AnalyzePlan RPC under Connect"))

    for n in (1_000, 100_000, 1_000_000):
        run.add(Measurement(f"collect_{n}", "ms",
                            timed(lambda n=n: spark.range(n).collect(),
                                  warmup=min(warmup, 10),
                                  trials=6 if n >= 1_000_000 else min(trials, 12)),
                            note="transport dominated"))

    run.add(Measurement("toPandas_100000", "ms",
                        timed(lambda: spark.range(100_000).toPandas(),
                              warmup=min(warmup, 10), trials=8),
                        note="Arrow path on both sides in 4.2"))

    for n in (1_000_000, 10_000_000):
        run.add(Measurement(f"aggregate_{n}", "ms",
                            timed(lambda n=n: spark.range(n)
                                  .withColumn("k", F.col("id") % 1000)
                                  .groupBy("k").agg(F.sum("id"), F.count("*"))
                                  .collect(),
                                  warmup=1, trials=5),
                            note="execution dominated"))

    spark.stop()
    return run


def report(runs: dict[str, Run]) -> None:
    classic, connect = runs.get("classic"), runs.get("connect")
    if not (classic and connect):
        raise SystemExit("need one classic run and one connect run to compare")

    by_name = {m.name: m for m in classic.measurements}
    print("\n" + "=" * 82)
    print("Spark Connect protocol overhead, single node, loopback, same core count")
    print(f"classic: Spark {classic.spark_version}   connect: Spark {connect.spark_version}")
    print("=" * 82)
    print(f"{'measurement':<22}{'classic':>12}{'connect':>12}{'delta':>12}{'ratio':>10}")
    print("-" * 82)

    for m in connect.measurements:
        c = by_name.get(m.name)
        if not c or c.median is None or m.median is None:
            continue
        delta = m.median - c.median
        ratio = (m.median / c.median) if c.median else float("inf")
        faster = "faster" if delta < 0 else "slower"
        print(f"{m.name:<22}{c.median:>11,.1f}{m.median:>12,.1f}"
              f"{delta:>+11,.1f}{ratio:>9.2f}x  {faster}")

    print("-" * 82)
    print("All figures are medians in milliseconds. Protocol overhead on loopback only: ")
    print("this says nothing about cluster scaling, real networks, or concurrent clients.")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").strip().splitlines()[0])
    ap.add_argument("--mode", choices=("classic", "connect"))
    ap.add_argument("--remote", default=os.getenv("SPARK_REMOTE"))
    ap.add_argument("--out", type=Path)
    ap.add_argument("--warmup", type=int, default=WARMUP,
                    help="warmup iterations discarded before timing (JIT control)")
    ap.add_argument("--trials", type=int, default=TRIALS)
    ap.add_argument("--compare", nargs=2, type=Path, metavar=("CLASSIC_JSON", "CONNECT_JSON"))
    args = ap.parse_args(argv)

    if args.compare:
        runs = {}
        for path in args.compare:
            data = json.loads(path.read_text())
            data["measurements"] = [Measurement(**m) for m in data["measurements"]]
            run = Run(**data)
            runs[run.mode] = run
        report(runs)
        return 0

    if not args.mode:
        ap.error("--mode is required unless --compare is given")

    run = run_bench(args.mode, args.remote, args.warmup, args.trials)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(asdict(run), indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
