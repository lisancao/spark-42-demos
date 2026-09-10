#!/usr/bin/env python3
"""Compare the tables written by the original and the migrated pipeline.

    SPARK_REMOTE=sc://localhost:15003 python parity.py --left iceberg_before --right iceberg

For each table, compares the schema, the row count and an order-independent content hash (each row
cast to strings and hashed with xxhash64, with the row hashes combined by bit_xor), because equal
row counts alone would miss changed values. `make parity` runs it; README.md shows the result.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass

# The ten tables the pipeline writes, in dependency order.
TABLES = (
    "bronze.dim_categories",
    "bronze.dim_brands",
    "bronze.dim_items",
    "bronze.dim_locations",
    "bronze.orders",
    "silver.orders_enriched",
    "silver.order_lifecycle",
    "gold.hourly_metrics",
    "gold.delivery_performance",
    "gold.brand_summary",
)

# A separator that cannot appear in a cast-to-string column value.
FIELD_SEP = "\x1f"
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
    def rows_match(self) -> bool:
        return self.left_rows is not None and self.left_rows == self.right_rows

    @property
    def content_match(self) -> bool:
        return self.left_hash is not None and self.left_hash == self.right_hash

    @property
    def ok(self) -> bool:
        return self.error is None and self.schema_match and self.rows_match and self.content_match

    def verdict(self) -> str:
        if self.error:
            return f"ERROR    {self.error}"
        if not self.schema_match:
            return "SCHEMA   schemas differ"
        if not self.rows_match:
            return f"ROWS     {self.left_rows} != {self.right_rows}"
        if not self.content_match:
            return f"CONTENT  same {self.left_rows} rows, different values"
        return f"ok       {self.left_rows:,} rows, hashes match"


def schema_fingerprint(spark, table: str) -> str:
    schema = spark.table(table).schema
    return "|".join(f"{f.name}:{f.dataType.simpleString()}:{f.nullable}" for f in schema)


def content_digest(spark, table: str) -> tuple[str, int]:
    """Return (order-independent content hash, row count) for a table."""
    from pyspark.sql import functions as F

    df = spark.table(table)
    cols = [F.coalesce(F.col(c).cast("string"), F.lit(NULL_SENTINEL)) for c in df.columns]
    row_hash = F.xxhash64(F.concat_ws(FIELD_SEP, *cols))
    agg = (
        df.select(row_hash.alias("h"))
        .agg(F.expr("bit_xor(h)").alias("xor"), F.count(F.lit(1)).alias("n"))
        .first()
    )
    # An empty table has a NULL xor; a fixed value makes two empty tables compare equal.
    return (str(agg["xor"]) if agg["xor"] is not None else "empty", int(agg["n"]))


def compare(
    spark, left_ns: str, right_ns: str, tables: tuple[str, ...] = TABLES
) -> list[TableParity]:
    results: list[TableParity] = []
    for rel in tables:
        left, right = f"{left_ns}.{rel}", f"{right_ns}.{rel}"
        try:
            left_schema = schema_fingerprint(spark, left)
            right_schema = schema_fingerprint(spark, right)
            lh, ln = content_digest(spark, left)
            rh, rn = content_digest(spark, right)
            results.append(
                TableParity(
                    table=rel,
                    left_rows=ln,
                    right_rows=rn,
                    left_hash=lh,
                    right_hash=rh,
                    schema_match=left_schema == right_schema,
                )
            )
        except Exception as exc:  # a table that cannot be read is reported as a failure
            results.append(
                TableParity(
                    table=rel,
                    left_rows=None,
                    right_rows=None,
                    left_hash=None,
                    right_hash=None,
                    schema_match=False,
                    error=f"{type(exc).__name__}: {str(exc).splitlines()[0][:150]}",
                )
            )
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Compare two pipeline runs for data parity.")
    ap.add_argument("--left", required=True,
                    help="catalog or namespace of the original run, for example iceberg_before")
    ap.add_argument("--right", required=True, help="namespace of the 'after' run")
    ap.add_argument("--remote", default=os.getenv("SPARK_REMOTE", "sc://localhost:15003"))
    ap.add_argument("--tables", nargs="*", default=list(TABLES))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(args.remote).create()
    results = compare(spark, args.left, args.right, tuple(args.tables))
    spark.stop()

    if args.json:
        print(json.dumps([asdict(r) | {"ok": r.ok} for r in results], indent=2))
    else:
        print(f"\nparity: {args.left}  vs  {args.right}")
        print("-" * 74)
        for r in results:
            print(f"  {r.table:<30} {r.verdict()}")
        passed = sum(1 for r in results if r.ok)
        print("-" * 74)
        print(f"  {passed}/{len(results)} tables identical")

    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
