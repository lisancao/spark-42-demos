# Case Study: Migrating the lakehouse-stack Pipeline to Spark Connect

This case study explains why `~/lakehouse-stack/scripts/pipelines/pipeline_spark41.py` was migrated
from Spark Classic to Spark Connect, and walks through the migration. The pipeline is 357 lines long
and materializes 10 Iceberg tables (5 bronze, 2 silver, 3 gold) through a decorator framework modeled
on Spark Declarative Pipelines. The changes are organized by the seven migration layers described in
§9 of the [companion guide](../../companion_guide.md). Results were verified against Apache Spark
4.2.0 on 2026-09-09.

> **Note** Running this case study requires the `~/lakehouse-stack` data and a locally built Iceberg
> runtime ([`BUILDING_ICEBERG_FOR_SPARK_4_2.md`](BUILDING_ICEBERG_FOR_SPARK_4_2.md)). The pipeline in
> [`pipeline/`](../../pipeline/MIGRATION.md) demonstrates the same layers without either.

`pipeline_before.py` is an unmodified copy of the production file below a 21-line header, so its body
is the original 357 lines and `wc -l` reports 378. `pipeline_after.py` is derived from it with
changes in the layers below. A diff of the two files shows nine hunks rather than one per layer,
because some layers add a comment alongside the code change. No transformation line changes, which is
what makes the comparison in `parity.py` meaningful: that the migration altered no business logic can
be checked rather than assumed.

## Background

### Why Migrate This Pipeline

The pipeline was written for the lakehouse-stack deployment, where each run is submitted with
`spark-submit` inside the Spark master container. That arrangement shows, in a single workload, the
driver coupling that Spark Connect was proposed to remove (companion guide §1):

- **The application must run beside the driver.** A script cannot be submitted from a laptop or a CI
  runner; it must first be copied into the container, which is why the repository's command-line
  tool contains copy-run-delete wrappers.
- **Every node that runs a driver holds catalog credentials.** The catalog password and object-storage
  keys are in a configuration file mounted into the master and every worker.
- **The application and the platform share one environment.** The pipeline's Python environment and
  Spark's JARs are those of the container, so upgrading Spark changes the environment every job runs
  in.

The pipeline is also a representative subject. Its transformations use only the DataFrame and SQL
APIs, and its decorator framework is modeled on Spark Declarative Pipelines
([SPARK-51727](https://issues.apache.org/jira/browse/SPARK-51727), Spark 4.1.0), so the changes it
needs are the ones most DataFrame pipelines need.

### Where the Layers Come From

The Spark Connect overview lists three ways in which a Connect client application differs from a
Spark Classic application. Each accounts for one or more of the migration layers (Table 1).

*Table 1. Documented differences between Connect and Classic applications, and the layers they produce*

| Difference, per the Spark Connect overview | Consequence | Layers |
|---|---|---|
| "The client does not run in the same process as the Spark driver." The client "cannot directly access and interact with the driver JVM." | No Py4J and no `SparkContext`; logs, file paths and checkpoints belong to the server; credentials are needed only where the driver runs | 1, 4, 5, 6 |
| The protocol uses Spark's logical plans as its abstraction, and "consequently … does not support all the execution APIs of Spark, most importantly RDDs." | No RDDs; names are sent unresolved and resolved against the server's catalogs | 1, 2 |
| "Spark Connect provides a session-based client." The client "does not have access to the static Spark configuration or the SparkContext." | A session is obtained differently, and configuration divides between client and server | 0, 3 |

The layers are therefore not an arbitrary checklist: each is a consequence of one of the three
documented design properties of Spark Connect.

**Citations:** [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html), [SPARK-39375](https://issues.apache.org/jira/browse/SPARK-39375), [SPARK-51727](https://issues.apache.org/jira/browse/SPARK-51727)

## Summary

*Table 2. Migration summary*

| Measure | Result |
|---|---|
| Lines in the pipeline | 357 |
| Lines of transformation logic changed | 0 |
| Blocking calls in the pipeline | 1 |
| Files in the repository with the same blocking call | 21 |
| RDD operations in the repository | 0 |
| Py4J access (`_jvm`, `_jsc`, `_jdf`) in the repository | 0 |
| Accumulators or broadcast variables in the repository | 0 |

The audit that produces these figures is `tools/compat_audit.py`. From this directory, `make audit`
runs it:

```bash
make audit    # python ../../tools/compat_audit.py ~/lakehouse-stack/scripts --fail-on NONE
```

Across the `scripts/` tree it reports 27 findings in 24 files: 21 blocking findings (all the same
`setLogLevel` call), 5 performance findings and 1 behavior finding. The last two categories are
discussed in "Findings Beyond Blocking Calls".

## Changes by Layer

### Layer 0: Session Creation

This is the one mandatory change. `build_session()` checks `SPARK_REMOTE`: when it is set, the
pipeline runs over Spark Connect; when it is not, the pipeline runs with Spark Classic as before.

```python
def build_session(name: str) -> SparkSession:
    remote = os.getenv("SPARK_REMOTE")
    builder = SparkSession.builder.appName(f"Pipeline_{name}")
    if remote:
        return builder.remote(remote).getOrCreate()
    if os.getenv("SPARK_API_MODE") == "connect":
        return builder.config("spark.api.mode", "connect").getOrCreate()
    return builder.getOrCreate()
```

The `spark.api.mode=connect` branch serves a different case from `SPARK_REMOTE`. `spark.remote`
connects to a running server (`sc://…`) or starts a local one (`local[*]`), but cannot take a cluster
master URL such as `spark://host:7077` and start a server on that cluster. `spark.api.mode` can, so an
existing `spark-submit` command keeps its `--master` and a single configuration value selects the
execution path. A job can be moved to Connect and back without a code change, and CI can run both
modes and compare their output.

### Layer 1: Pipeline Logic

No pipeline logic changes. The framework infers table dependencies by reading its own source:

```python
source = inspect.getsource(func)
deps = set(re.findall(r'spark\.table\(["\']([^"\']+)["\']\)', source))
```

This code runs unchanged, because it is client-side Python that never calls Spark. The topological
sort built on it is also unchanged, as are the pipeline's `withColumn`, `groupBy`, `agg`,
`f.broadcast` join hints and `saveAsTable` calls.

### Layer 2: Data Sources and Catalogs

The pipeline's use of the catalog does not change; where the catalog is configured does.

`spark.sql.catalog.iceberg` and `spark.sql.extensions` must be set on the server, because their
values are Java class names and the Connect client has no JVM in which to load them. Setting
`spark.sql.catalog.iceberg` from the client produces no error; the failure appears when the catalog is
first used.

The session lives on the server, and the client holds a reference to it. When the pipeline calls
`spark.table("iceberg.silver.orders_enriched")`, the table name is sent as an unresolved string and
the server resolves it against its own catalog manager. Nothing is preloaded or cached on the client,
and the client never reads Iceberg metadata, Parquet files or object storage directly. Only result
rows reach the client, as Arrow.

File paths now resolve on the server. The bronze functions read:

```python
return spark.read.parquet("/data/dimensions/categories.parquet")
```

This works because the case study's Connect server mounts the lakehouse-stack data at `/data`. If
the server runs on a host without that mount, all five bronze tables fail with a path-not-found error
that does not point to the migration as the cause.

### Layer 3: Configuration and Preflight

```python
def preflight(spark, catalog: str) -> None:
    root = catalog.split(".")[0]
    try:
        spark.sql(f"SHOW NAMESPACES IN {root}").count()
    except Exception as exc:
        raise SystemExit(...)
```

Under Connect the client does not determine the runtime, so two endpoints that accept the same
connection string can offer different capabilities. Checking at startup turns a catalog error in the
middle of a gold-layer write into a refusal to start with a specific message.

Table 3 shows which configuration a client can set.

*Table 3. Configuration a Connect client can and cannot set*

| Configuration | Settable from the client | Reason |
|---|---|---|
| `spark.sql.shuffle.partitions` | Yes | A value the server reads at run time |
| `spark.sql.extensions` | No | Read once, when the session is created |
| `spark.jars` | No | The class path is fixed when the server starts |
| `spark.sql.catalog.iceberg` | In effect, no | Resolved lazily, so a runtime setting registers the name; but the JAR must already be on the server, and without `spark.sql.extensions`, `MERGE INTO` and `CALL iceberg.system.*` are unavailable |

### Isolating Test Runs

```python
DEFAULT_CATALOG = os.getenv("PIPELINE_CATALOG", "iceberg")
```

The original pipeline hard-codes `catalog="iceberg"`, so running it overwrites the production bronze,
silver and gold tables. Making the catalog configurable is necessary, but it does not isolate a run
on its own.

`Pipeline.run` builds each write target as `f"{self.catalog}.{table_name}"`, so `PIPELINE_CATALOG`
redirects writes. It does not redirect reads: the silver and gold functions call
`spark.table("iceberg.bronze.orders")` with the catalog name in the transformation body, and those
bodies are deliberately identical in both files. A run with `PIPELINE_CATALOG=iceberg.parity_after`
would therefore write to the new namespace while reading the previous run's bronze tables. Silver and
gold would derive from the same inputs, and the comparison would pass without testing the migration.

The runs are isolated instead with **two warehouses under one catalog name**. Both runs use the
catalog `iceberg`, and each runs against a server whose `iceberg` catalog points at a different
warehouse directory. This requires no pipeline change, and it corresponds to running the old pipeline
against production and the new one against staging. Under Connect, where data lives is a property of
the server the client connects to, so isolating runs means isolating endpoints.

### Layer 4: Dependencies

This pipeline defines no Python UDFs, so no dependencies need to be shipped with `addArtifact`. The
pipeline in [`pipeline/`](../../pipeline/MIGRATION.md) ships one.

### Layer 5: Execution and Operations

The pipeline's one blocking call belongs to this layer:

```diff
-            self._spark = SparkSession.builder \
-                .appName(f"Pipeline_{self.name}") \
-                .getOrCreate()
-            self._spark.sparkContext.setLogLevel("WARN")
+            self._spark = build_session(self.name)
```

The Connect client rejects `spark.sparkContext` by name: `session.py` in PySpark 4.2 raises
`JVM_ATTRIBUTE_NOT_SUPPORTED` for `_jsc`, `_jconf`, `_jvm`, `_jsparkSession`, `sparkContext` and
`newSession`. `setLogLevel` has no client-side equivalent, because the log level belongs to the
server's JVM. Configure `log4j2` where the Connect server starts, or remove the call.

The same call appears in 21 files across `lakehouse-stack/scripts/`, so removing it is a
repository-wide change. It is also the repository's entire blocking surface.

Job submission also changes. Before the migration:

```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/pipelines/pipeline_spark41.py
```

The script had to be visible inside the container, so the repository's command-line tool contains a
wrapper (`lakehouse:955-959`):

```bash
cp data/load_to_iceberg.py scripts/_load_to_iceberg.py
docker exec "$master_container" /opt/spark/bin/spark-submit /scripts/_load_to_iceberg.py
rm -f scripts/_load_to_iceberg.py
```

After the migration, the client runs on the host and the wrapper is unnecessary:

```bash
SPARK_REMOTE=sc://localhost:15003 python pipeline_after.py
```

Two operational changes produce no error:

- **Log locations.** Driver logs are server logs. Output from `print()` inside a `ForeachWriter`, such
  as the `LatencyMetricsWriter` in `scripts/demos/showcase/realtime_mode.py`, appears in executor
  logs rather than the client's terminal.
- **Checkpoint locations.** Checkpoint paths resolve on the server. In this deployment,
  `spark-defaults.conf` notes that S3A checkpoints against SeaweedFS fail, so checkpoints remain on
  `file://` paths.

### Layer 6: Security and Identity

In this deployment, `config/spark/spark-defaults.conf` contains catalog credentials and is mounted
into the master and every worker. Under Connect those credentials are needed only by the Connect
server. The client connects to a gRPC endpoint and holds no catalog credentials, so laptops, CI
runners and notebooks no longer need them.

Spark Connect has no built-in authentication; per the overview documentation, it is intended to run
behind an authenticating gRPC proxy. Setting `token` in the connection string enables TLS. Sessions
are local to a server, so a Service that load-balances across several Connect replicas requires
session affinity.

## Verifying the Migration

### Running Both Versions

One server registers both warehouses, so a single session can compare them. The Makefile in this
directory contains the full commands:

```bash
make up              # Connect server on port 15003; catalogs iceberg (/warehouse_after)
                     # and iceberg_before (/warehouse)
make migrate-before  # Spark Classic: spark-submit inside the container, writing to /warehouse
make migrate-after   # Spark Connect: pipeline_after.py from the host, writing to /warehouse_after
make parity          # compare all ten tables across the two warehouses
```

### Results

Verified on 2026-09-09 against Spark 4.2.0 with the locally built Iceberg runtime, using
`orders_1d.parquet` (74 MB) mounted at the path the pipeline expects:

```
parity: iceberg_before  vs  iceberg
--------------------------------------------------------------------------
  bronze.dim_categories          ok       10 rows, hashes match
  bronze.dim_brands              ok       20 rows, hashes match
  bronze.dim_items               ok       160 rows, hashes match
  bronze.dim_locations           ok       4 rows, hashes match
  bronze.orders                  ok       1,027,129 rows, hashes match
  silver.orders_enriched         ok       1,009,933 rows, hashes match
  silver.order_lifecycle         ok       37,282 rows, hashes match
  gold.hourly_metrics            ok       118 rows, hashes match
  gold.delivery_performance      ok       10 rows, hashes match
  gold.brand_summary             ok       21 rows, hashes match
--------------------------------------------------------------------------
  10/10 tables identical
```

Two independent runs, one with Spark Classic and one over Spark Connect, produced identical content
hashes for all ten tables.

### How Parity Is Checked

`parity.py` compares each table on schema, row count and an order-independent content hash:
`xxhash64` per row, combined with `bit_xor`, with fields joined by a separator that cannot occur in the
data. The hash is needed because a row count alone does not detect a result with the same number of
rows but different values, which is what an unintentionally changed join produces. The project's
`pipeline/parity.py` uses the same method, and `tests/spark/test_parity.py` constructs each of these
failure cases for it.

The migration is complete when all ten tables match.

### Startup Warning for Session Extensions

Both the `spark-submit` run and the Connect server log the following at startup:

```
WARN SparkSession: Cannot use org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
to configure session extensions.
java.lang.ClassNotFoundException: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

The warning is expected and does not indicate a problem. It applies to the server's bootstrap
session, which is created before `--jars` are on the class path; client sessions created afterward
load the extensions normally. With `--jars` alone, a Connect client session runs both `MERGE INTO`
and `CALL <catalog>.system.expire_snapshots(...)`, each of which requires the extensions. Adding
`--driver-class-path` does not change the warning.

Verify session extensions by using them rather than by reading the log. `verify_iceberg_runtime.py`
runs `MERGE INTO` and `CALL <catalog>.system.expire_snapshots`, both of which require them.

## Findings Beyond Blocking Calls

The audit's non-blocking findings depend on program structure rather than on names, which is why the
audit parses code rather than searching text:

```
scripts/demos/showcase/spark_cluster_diagnostic.py:242  [BEHAVIOR/L1]  temp view 'brands' registered 2 times
scripts/demos/mlflow-agents/analyst.py:79               [PERF/L1]      .schema inside a loop
scripts/demos/mlflow-agents/autopilot.py:115            [PERF/L1]      .columns inside a loop
scripts/demos/mlflow-agents/guardian.py:139             [PERF/L1]      .columns inside a loop
scripts/demos/showcase/otf_portability.py:62            [PERF/L1]      .schema inside a loop
scripts/quickstarts/iceberg-spark-quickstart.py:131     [PERF/L1]      .schema inside a loop
```

The temporary view finding is a correctness issue that exists only under Connect. Spark Classic
captures a temporary view's plan when the view is registered; Spark Connect resolves the view by name
at execution. Re-registering `brands` therefore changes every DataFrame built on the earlier
definition. The five performance findings are schema accesses inside loops: under Connect, schema
analysis is lazy and each access to a new DataFrame's schema is an `AnalyzePlan` call.

## Order of Operations

Carry out the layers in dependency order:

1. **Layers 2 and 3.** Stand up a correctly configured Connect server, and confirm it with
   `preflight`.
2. **Layer 0.** Point one job at the server, using `spark.api.mode` so that the change is reversible.
3. **Layers 1 and 5.** Run the audit, fix the blocking findings (here, `setLogLevel`), review the
   behavior findings, relocate logging and checkpoint paths, and retire the `docker exec` wrappers.
4. **Layer 4.** Move dependency distribution to `addArtifact`. This pipeline has no UDFs, so there is
   nothing to move.
5. **Layer 6.** Remove credentials from clients. This step is last because it is the least easily
   reversed.

Keep the Spark version upgrade separate from the change of architecture. Spark 4.2 has its own
behavior changes: the minimum PyArrow version becomes 18.0.0, Arrow-optimized Python UDFs and Arrow
IPC become the default ([SPARK-54555](https://issues.apache.org/jira/browse/SPARK-54555)), and PyPy is
no longer supported. These are easier to diagnose in isolation.
