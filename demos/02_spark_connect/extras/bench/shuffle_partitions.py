#!/usr/bin/env python3
"""Measure how spark.sql.shuffle.partitions affects collecting a small aggregated result.

    python extras/bench/shuffle_partitions.py --mode connect --remote sc://localhost:15002
    python extras/bench/shuffle_partitions.py --mode classic   # full pyspark and a JDK

Results: companion guide §13, "Aggregations and Partition Count".
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from pathlib import Path

PARTITION_COUNTS = (8, 32, 200, 800)
ROWS = 10_000_000
GROUPS = 1_000
WARMUP, TRIALS = 3, 7


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").strip().splitlines()[0])
    ap.add_argument("--mode", choices=("classic", "connect"), required=True)
    ap.add_argument("--remote", default=os.getenv("SPARK_REMOTE"))
    ap.add_argument("--out", type=Path)
    args = ap.parse_args(argv)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    if args.mode == "connect":
        spark = SparkSession.builder.appName("bench-shuffle").remote(args.remote).getOrCreate()
    else:
        spark = SparkSession.builder.appName("bench-shuffle").master("local[*]").getOrCreate()

    print(f"mode={args.mode}  spark={spark.version}  rows={ROWS:,}  groups={GROUPS}")
    print(f"  {'partitions':>10} {'median ms':>12}")
    results = {}
    for parts in PARTITION_COUNTS:
        spark.conf.set("spark.sql.shuffle.partitions", str(parts))

        def job():
            return (
                spark.range(ROWS)
                .withColumn("k", F.col("id") % GROUPS)
                .groupBy("k")
                .agg(F.sum("id"), F.count("*"))
                .collect()
            )

        for _ in range(WARMUP):
            job()
        samples: list[float] = []
        row_count = 0
        for _ in range(TRIALS):
            t0 = time.perf_counter()
            rows = job()
            samples.append((time.perf_counter() - t0) * 1000)
            row_count = len(rows)
        assert row_count == GROUPS, f"expected {GROUPS} groups, got {row_count}"
        med = statistics.median(samples)
        results[parts] = med
        print(f"  {parts:>10} {med:>12,.1f}")

    spark.stop()
    if args.out:
        args.out.write_text(json.dumps({"mode": args.mode, "spark": spark.version,
                                        "rows": ROWS, "groups": GROUPS,
                                        "median_ms_by_partitions": results}, indent=2))
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
