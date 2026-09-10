"""Compare two runs of the orders pipeline table by table: schema, row count and content hash.

    python pipeline/parity.py \
        --left /opt/spark/work-dir/out/classic --right /opt/spark/work-dir/out/connect

The Spark server reads both paths. The content hash does not depend on row order or partitioning:
each row is hashed with xxhash64 over a null-safe string form of every column, and the row hashes
are combined with bit_xor.
"""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLES = (
    "bronze_events",
    "bronze_stores",
    "bronze_brands",
    "silver_orders_enriched",
    "silver_order_lifecycle",
    "gold_delivery_performance",
    "gold_brand_summary",
)
FIELD_SEP = "\x1f"  # cannot occur in a column value cast to string
NULL_SENTINEL = "\x00NULL"


@dataclass
class TableParity:
    table: str
    left_rows: int | None
    right_rows: int | None
    left_hash: str | None
    right_hash: str | None
    schema_match: bool
    error: str | None = None

    @property
    def ok(self) -> bool:
        return (
            self.error is None
            and self.schema_match
            and self.left_rows == self.right_rows
            and self.left_hash == self.right_hash
        )

    def verdict(self) -> str:
        if self.error:
            return f"ERROR    {self.error}"
        if not self.schema_match:
            return "SCHEMA   schemas differ"
        if self.left_rows != self.right_rows:
            return f"ROWS     {self.left_rows} != {self.right_rows}"
        if self.left_hash != self.right_hash:
            return f"CONTENT  same {self.left_rows} rows, different values"
        return f"ok       {self.left_rows:,} rows, hashes match"


def schema_fingerprint(df: DataFrame) -> str:
    return "|".join(f"{f.name}:{f.dataType.simpleString()}:{f.nullable}" for f in df.schema)


def content_digest(df: DataFrame) -> tuple[str, int]:
    """Return an order-independent hash of every row, and the row count."""
    values = [F.coalesce(F.col(c).cast("string"), F.lit(NULL_SENTINEL)) for c in df.columns]
    row_hash = F.xxhash64(F.concat_ws(FIELD_SEP, *values))
    result = (
        df.select(row_hash.alias("h"))
        .agg(F.expr("bit_xor(h)").alias("xor"), F.count(F.lit(1)).alias("n"))
        .collect()[0]
    )
    # bit_xor of no rows is NULL, so two empty tables compare equal.
    return (str(result["xor"]) if result["xor"] is not None else "empty", int(result["n"]))


def compare_frames(name: str, left: DataFrame, right: DataFrame) -> TableParity:
    left_hash, left_rows = content_digest(left)
    right_hash, right_rows = content_digest(right)
    return TableParity(
        table=name,
        left_rows=left_rows,
        right_rows=right_rows,
        left_hash=left_hash,
        right_hash=right_hash,
        schema_match=schema_fingerprint(left) == schema_fingerprint(right),
    )


def compare(
    spark: SparkSession, left_root: str, right_root: str, tables=TABLES
) -> list[TableParity]:
    results = []
    for name in tables:
        try:
            left = spark.read.parquet(f"{left_root}/{name}")
            right = spark.read.parquet(f"{right_root}/{name}")
            results.append(compare_frames(name, left, right))
        except Exception as exc:  # a table that cannot be read fails parity
            error = str(exc).splitlines()[0][:120]
            results.append(TableParity(name, None, None, None, None, False, error))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare two runs of the orders pipeline.")
    parser.add_argument("--left", required=True, help="output root of the first run")
    parser.add_argument("--right", required=True, help="output root of the second run")
    parser.add_argument("--remote", default=os.environ.get("SPARK_REMOTE", "sc://localhost:15002"))
    args = parser.parse_args()

    spark = SparkSession.builder.remote(args.remote).create()
    results = compare(spark, args.left, args.right)
    spark.stop()

    print(f"parity: {args.left}  vs  {args.right}")
    for result in results:
        print(f"  {result.table:<28} {result.verdict()}")
    passed = sum(result.ok for result in results)
    print(f"  {passed}/{len(results)} tables identical")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
