"""Check that a locally built Iceberg runtime works on a Spark 4.2 Connect server.

    python verify_iceberg_runtime.py sc://localhost:15003               # or: make verify-iceberg
    python verify_iceberg_runtime.py sc://localhost:15012 --catalog ice

Apache Iceberg publishes no runtime for Spark 4.2; BUILDING_ICEBERG_FOR_SPARK_4_2.md describes the
build. Exits with status 0 when every check passes. View checks are skipped on a HadoopCatalog,
which does not support views (BUILDING_ICEBERG_FOR_SPARK_4_2.md).
"""
import argparse
import sys

from pyspark.sql import SparkSession

ap = argparse.ArgumentParser(description=__doc__)
ap.add_argument("remote", nargs="?", default="sc://localhost:15003", help="sc:// URL of the server")
ap.add_argument("--catalog", default="iceberg", help="Iceberg catalog name to exercise")
ap.add_argument("--namespace", default="v", help="scratch namespace to create and use")
args = ap.parse_args()

cat, ns = args.catalog, args.namespace
tbl = f"{cat}.{ns}.t"

spark = SparkSession.builder.remote(args.remote).appName("ice42-verify").create()
print(f"server:  {spark.version}")
print(f"catalog: {cat}  (type: {spark.conf.get(f'spark.sql.catalog.{cat}.type', 'unset')})")


def step(label, fn):
    try:
        r = fn()
        print(f"  PASS  {label}" + (f"  -> {r}" if r is not None else ""))
        return True
    except Exception as e:
        print(f"  FAIL  {label}: {type(e).__name__}: {str(e).splitlines()[0][:110]}")
        return False


res = {}
res["namespace"] = step("CREATE NAMESPACE",
    lambda: spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{ns}") and None)
res["write"] = step("write iceberg table", lambda: (
    spark.createDataFrame([(1, "a", 1.5), (2, "b", 2.5), (3, "c", 3.5)],
                          "id int, k string, v double")
         .writeTo(tbl).using("iceberg").createOrReplace(), None)[1])
res["read"] = step("read back", lambda: spark.table(tbl).count())
res["merge"] = step("MERGE INTO (requires session extensions)", lambda: (spark.sql(f"""
    MERGE INTO {tbl} t USING (SELECT 2 AS id, 'b' AS k, 99.0 AS v) s
    ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v
    WHEN NOT MATCHED THEN INSERT *"""), None)[1])
res["merge_effect"] = step("MERGE applied",
    lambda: spark.sql(f"SELECT v FROM {tbl} WHERE id=2").first()[0])
res["procedure"] = step(f"CALL {cat}.system.expire_snapshots",
    lambda: spark.sql(f"CALL {cat}.system.expire_snapshots(table => '{ns}.t')").count())
res["metadata"] = step("metadata table .snapshots", lambda: spark.table(f"{tbl}.snapshots").count())
res["schema_evo"] = step("ALTER TABLE ADD COLUMN", lambda: (
    spark.sql(f"ALTER TABLE {tbl} ADD COLUMN extra string"), None)[1])


def _time_travel():
    # VERSION AS OF takes a literal rather than a subquery, so look up the snapshot id first.
    sid = spark.sql(f"SELECT min(snapshot_id) AS m FROM {tbl}.snapshots").first()["m"]
    return spark.sql(f"SELECT count(*) c FROM {tbl} VERSION AS OF {sid}").first()["c"]


res["time_travel"] = step("time travel (VERSION AS OF <literal>)", _time_travel)
res["delete"] = step("DELETE FROM (row-level)",
    lambda: (spark.sql(f"DELETE FROM {tbl} WHERE id = 3"), None)[1])

# Views: only meaningful on a catalog type that supports them.
catalog_type = spark.conf.get(f"spark.sql.catalog.{cat}.type", "unset")
if catalog_type == "hadoop":
    print(f"  SKIP  views: catalog '{cat}' is a HadoopCatalog, which does not support views. "
          "Use a jdbc, hive or rest catalog to check views.")
    skipped = ["view"]
else:
    skipped = []
    res["view"] = step("CREATE VIEW / SELECT / SHOW VIEWS / REPLACE / DROP", lambda: (
        spark.sql(f"CREATE OR REPLACE VIEW {cat}.{ns}.vw AS SELECT * FROM {tbl}"),
        spark.sql(f"SELECT count(*) FROM {cat}.{ns}.vw").first(),
        spark.sql(f"SHOW VIEWS IN {cat}.{ns}").collect(),
        spark.sql(f"CREATE OR REPLACE VIEW {cat}.{ns}.vw AS SELECT id FROM {tbl}"),
        spark.sql(f"DROP VIEW {cat}.{ns}.vw"), None)[-1])

print("\n--- summary ---")
for k, v in res.items():
    print(f"  {'PASS' if v else 'FAIL'}  {k}")
for k in skipped:
    print(f"  SKIP  {k}")
bad = [k for k, v in res.items() if not v]
print(f"\nfailures: {bad if bad else 'none'}")
spark.stop()
sys.exit(1 if bad else 0)
