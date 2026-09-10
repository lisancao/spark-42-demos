# Measurements and Test Results

Every figure quoted in the companion guide, the README and the video script comes from this file.
All of it was measured on 2026-09-09 and 2026-09-10 against Apache Spark **4.2.0** (released
2026-07-14) on Linux 6.17. Client results use CPython 3.10.20 unless a section states otherwise; §3
used CPython 3.12.3. The programs that produced the results are in `examples/`, `pipeline/`,
`tools/`, `extras/` and `case_study/lakehouse_stack/`.

Anything not measured here is marked in the guide as either documented-but-not-exercised, or
untested.

---

## 1. PySpark Packaging

Four PyPI packages, measured by installing each into a clean virtualenv with uv on CPython 3.10.20
(bytecode compiled) and running `du -sh` on the result.

| Package | PyPI artifact | `pyspark/` installed | jars | py4j | `spark.remote` | starts a local server |
|---|---|---|---|---|---|---|
| `pyspark` | 429.28 MB sdist | 481 MB | 456 MB, 276 JAR files | yes | no | no |
| `pyspark[connect]` | 429 MB + extras | 481 MB | 456 MB | yes | yes | yes |
| `pyspark-connect` | 0.01 MB | installs `pyspark` | 456 MB | yes | yes | yes |
| `pyspark-client` | **1.60 MB** | **19 MB** | **none** | **no** | yes | no |

`pyspark/jars` holds 277 entries: 276 JAR files and a `connect-repl` directory.

`pyspark-connect` is a metapackage. Its `requires_dist` in the PyPI JSON API for 4.2.0 is
`pyspark==4.2.0`, `pandas>=2.2.0`, `pyarrow>=18.0.0`, `grpcio>=1.76.0`, `grpcio-status>=1.76.0`,
`googleapis-common-protos>=1.71.0`, `zstandard>=0.25.0`, `numpy>=1.21` and `pyyaml>=3.11`, so
installing it pulls the full distribution. The thin client is `pyspark-client`.

`pyspark-client` provides the `pyspark` namespace: `import pyspark` works, `pyspark.__version__`
reports `4.2.0`, and `from pyspark.sql import SparkSession, functions` resolves. It has no
`pyspark/jars` directory and no `py4j`.

### Composition of a `pyspark-client` Environment

| Package | Installed |
|---|---|
| `pyarrow` 25.0.1 | 154 MB |
| `pandas` 2.3.3 | 65 MB |
| `numpy` 2.2.6 | 41 MB |
| `zstandard` | 23 MB |
| **`pyspark`** | **19 MB** |
| `grpc` 1.83.1 | 18 MB |

A working `pyspark-client` virtualenv is **359 MB**, because `pyarrow`, `pandas` and `numpy` are
required dependencies of the Connect client. A `pyspark[connect]` virtualenv is 820 MB, and a plain
`pyspark` virtualenv, which installs neither pandas nor pyarrow, is 483 MB. The defensible summary is
*no JVM, no JDK, no jars*, not *small*.

The project's environments from `make setup` also contain the development tools (pytest and ruff):
`.venv` measures 396 MB and `.venv-full` 857 MB, each measured with its own `du` run, because uv
hard-links files from its cache and `du` counts a shared file once per invocation.

### Version History

`pyspark` sdist: 3.5.0 = 302 MB, 4.0.0 = 414 MB, 4.1.0 = 434 MB, 4.2.0 = 429 MB.
`pyspark-client`: 4.0.0 = 1.43 MB, 4.1.0 = 1.53 MB, 4.2.0 = 1.60 MB.

The documented thin-client install changed between releases: the 4.0.0 Connect overview said
`pyspark[connect]`; 4.1 and 4.2 say `pyspark-client`. The package split is
[SPARK-51212](https://issues.apache.org/jira/browse/SPARK-51212).

---

## 2. API Behavior

Verified against a live 4.2.0 Connect server with `pyspark-client==4.2.0`.

| Behavior | Result |
|---|---|
| `spark.sparkContext`, `newSession`, `_jvm`, `_jsc`, `_jsparkSession`, `_jconf` | raise `PySparkAttributeError` / `JVM_ATTRIBUTE_NOT_SUPPORTED` |
| `DataFrame.rdd` | same error class: not a distinct `NOT_IMPLEMENTED` |
| `SparkSession.Builder.enableHiveSupport()` | **does not raise.** Session builds; `UserWarning [CANNOT_MODIFY_STATIC_CONFIG]`; `spark.sql.catalogImplementation` stays `in-memory` |
| `spark.conf.get("spark.api.mode")` when unset | raises `SparkNoSuchElementException [SQL_CONF_NOT_FOUND]`. `classic` is the behavior when absent, not a stored value |
| client-side `.config("spark.sql.extensions", …)` | `UserWarning [CANNOT_MODIFY_STATIC_CONFIG]` (visible) |
| client-side `.config("spark.sql.catalog.<name>", …)` | silent; reads back, fails only when the catalog is used |
| `functions.broadcast(df)`, `foreach`, `foreachPartition` | work |
| `SparkContext.accumulator`, `sc.broadcast` | unreachable (constructed via `SparkContext`) |
| `Observation.get` | a property returning `dict`; `obs.get["n"]` is correct |
| `DataFrame.zipWithIndex()` | present ([SPARK-55229](https://issues.apache.org/jira/browse/SPARK-55229)) |
| `spark.read.json(dataframe)` | accepts a DataFrame ([SPARK-56253](https://issues.apache.org/jira/browse/SPARK-56253)) |
| `SparkSession.emptyDataFrame` | a **method taking a schema**, not a property ([SPARK-56256](https://issues.apache.org/jira/browse/SPARK-56256)) |
| `spark.connect.grpc.port.maxRetries` | `0`: a port clash is a hard failure |
| Both `spark.connect.grpc.binding.host` and `.address` | registered configs; `host` is the one to set |
| Server started with neither binding setting (official 4.2.0 image, `start-connect-server.sh --wait`) | listens on all interfaces: the only listening socket for port 15002 in `/proc/net/tcp6` is `[::]:15002` (measured 2026-09-10) |
| Invalid column in `select()` (`examples/10_eager_and_lazy.py`) | Spark Classic: raises when the DataFrame is defined. Spark Connect: raises when an action runs |
| Temp view `t` replaced (3 rows, then 7) after a DataFrame was read from it | Spark Classic: the earlier DataFrame counts 3 rows. Spark Connect: it counts 7 |
| A Python UDF defined in a loop over `(1, 2, 3)`, returning `v * factor` | Spark Classic: `x1=1, x2=2, x3=3`. Spark Connect: `x1=3, x2=3, x3=3` |

### Session Creation

`getOrCreate()` returns the process's cached active session **even when the builder names a
different `remote` URL**, with no error or warning:

```python
a = SparkSession.builder.remote("sc://good-server:15002").getOrCreate()
b = SparkSession.builder.remote("sc://127.0.0.1:1").getOrCreate()
assert b is a     # passes
```

`builder.create()` forces a new server-side session. Two sessions created that way are isolated:
each sees only its own temp view of a shared name, and stopping one leaves the other running.

`create()` rejects a `local*` remote with `UNSUPPORTED_LOCAL_CONNECTION_STRING`, so in-process mode
requires `getOrCreate()`. Detect that by catching the error rather than by testing the URL string:
`"localhost".startswith("local")` is true.

| Case | Result |
|---|---|
| `SPARK_REMOTE` set, full `pyspark`, `builder.master("local[*]").getOrCreate()` | raises `[CANNOT_CONFIGURE_SPARK_CONNECT_MASTER]` |
| `SPARK_REMOTE` set, full `pyspark`, `builder.getOrCreate()` | a Spark Connect session (`pyspark.sql.connect.session`) |
| `pyspark-client`, `SPARK_REMOTE` unset, `builder.getOrCreate()` in a script | raises `PySparkRuntimeError [CONNECT_URL_NOT_SET]` |
| `pyspark-client`, `builder.remote("sc://localhost:15002").create()` with nothing listening on the port | blocks without output; the process had not returned after about 18 minutes, when it was stopped |

### In-Process Mode (`spark.remote=local[*]`)

The server binds `spark.connect.grpc.binding.port`, but the **client always connects to
`localhost:15002`**. Changing the binding port therefore separates the two: with
`binding.port=15014`, the local server listened on 15014, `spark.client._builder.endpoint` reported
`localhost:15002`, and `spark.version` returned **4.1.0** from an unrelated server that owned that
port, even though the virtualenv's own JARs were 4.2.0. Nothing raised.

In-process mode requires port 15002 to be free. The `BindException` you get when it is occupied is
the safe failure.

The session is genuinely the Connect code path: `type(spark).__module__` is
`pyspark.sql.connect.session` and `spark.sparkContext` raises, exactly as against a remote server.

### Python Version Coupling and the Official Images

Client and executor Python minor versions must match, or Python UDFs fail with
`[PYTHON_VERSION_MISMATCH]`: `examples/06_python_versions.py` run with Python 3.12 reports a worker
on 3.10 and a driver on 3.12.

| Observation | Result |
|---|---|
| Python in `apache/spark:4.2.0-scala2.13-java21-python3-ubuntu` and `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu` | 3.10.12; `pyarrow`, `pandas` and `numpy` not installed |
| Python UDF on the unmodified official 4.2.0 image | fails on the executor with `ModuleNotFoundError: No module named 'pyarrow'`; the server reports `spark.sql.execution.pythonUDF.arrow.enabled=true`. `toPandas()` of 1,000 rows works |
| Same server, `spark.sql.execution.pythonUDF.arrow.enabled=false` set in the session before the UDF is defined | the UDF works; `addArtifact` fails before shipping and works after |
| Same server, the setting applied after the UDF is defined | the UDF still fails with `ModuleNotFoundError: No module named 'pyarrow'`; the client reads the setting when the UDF is created (`pyspark/sql/connect/udf.py`, line 65) |
| Same server, `@udf("int", useArrow=False)` with default settings | the UDF works |
| Official 4.2.0 image with `pyarrow` and `pandas<3` installed | the UDF works with default settings |
| Unmodified official 4.1.2 image, 4.1.2 client | the UDF works; `pythonUDF.arrow.enabled` is `false` by default |
| Spark Classic `local` mode from a 3.10 virtualenv, `PYSPARK_PYTHON` unset | Python workers start `python3` from `PATH`; with 3.12 on `PATH`, `[PYTHON_VERSION_MISMATCH]` |

On CPython 3.12, `pyspark-client==4.2.0` resolved pandas 3.0.5, which PySpark warns against
(*"does not yet fully support pandas >= 3.0.0"*). On CPython 3.10 it resolved pandas 2.3.3. The
project pins `pandas<3`.

---

## 3. Protocol Performance

Harnesses: `extras/bench/protocol_overhead.py` and `extras/bench/shuffle_partitions.py`, run with
CPython 3.12.3 on 16 cores (recorded in `extras/bench/*.json`).

**Scope.** Single node, loopback, one client. Both modes ran **in the same container** with the same
16 cores and the same Spark build, which isolates the cost of the client/server split. These figures
say nothing about cluster scaling, real network latency, or concurrent clients.

**Method.** 50 warmup iterations discarded, 20 timed trials, medians reported. The Connect server was
restarted immediately before its run so neither side was long-warmed. Warmup dominates at this
timescale: at 3 warmups `trivial_query` measured 32.3 ms on a freshly started classic JVM against
7.5 ms on a Connect server that had been running for hours, which compares an interpreted JVM to a
JIT-compiled one rather than measuring the protocol.

| Measurement | Classic | Connect | Δ |
|---|---|---|---|
| `session_start` | 2,530 ms | 515 ms | −2,015 ms (4.9× faster) |
| `trivial_query` | 14.4 ms | 15.3 ms | +0.9 ms |
| `sql_select_1` | 9.7 ms | 11.3 ms | +1.6 ms |
| `schema_access` | 1.7 ms | 1.1 ms | −0.6 ms |
| `collect` 1k rows | 14.5 ms | 16.8 ms | +2.3 ms |
| `collect` 100k rows | 121.4 ms | 120.4 ms | −1.0 ms |
| `collect` 1M rows | 1,401 ms | 1,286 ms | −115 ms |
| `toPandas` 100k | 12.3 ms | 14.6 ms | +2.3 ms |
| `aggregate` 1M rows | 145.9 ms | 220.1 ms | +74.2 ms |
| `aggregate` 10M rows | 136.4 ms | 195.9 ms | +59.5 ms |

### Shuffle Partition Sensitivity

10M rows aggregated into 1,000 groups, varying only `spark.sql.shuffle.partitions`:

| Partitions | Classic | Connect |
|---|---|---|
| 8 | 93.8 ms | 60.1 ms |
| 32 | 70.4 ms | 70.7 ms |
| 200 (default) | 139.7 ms | 169.3 ms |
| 800 | 115.7 ms | 270.2 ms |

Connect's cost rises monotonically with partition count; classic's does not. This is consistent with
per-partition Arrow batch overhead on the result stream. The practical consequence: reduce the
partition count before collecting a small result from a wide shuffle.

Classic's series is non-monotonic at 7 trials per point, so only the 8- and 800-partition endpoints
support a confident cross-mode claim. The Connect series is clean across all four.

**Not measured:** concurrency, real networks, cluster execution, Python UDF throughput, streaming.

---

## 4. Iceberg on Spark 4.2

Apache Iceberg publishes no Spark 4.2 runtime: Maven Central stops at
`iceberg-spark-runtime-4.1_2.13`, and the source tree has modules for 3.5, 4.0 and 4.1 only.

### The Released 4.1 Runtime on Spark 4.2

Loading it fails at class-load time, and because `SparkCatalog` references the failing class, the
whole catalog is disabled, and all eleven capability checks fail rather than only the view checks:

```
java.lang.IncompatibleClassChangeError: class org.apache.iceberg.spark.source.SparkView
can not implement org.apache.spark.sql.connector.catalog.View, because it is not an interface
```

| | `org.apache.spark.sql.connector.catalog.View` |
|---|---|
| Spark 4.1.2 | `public interface View` |
| Spark 4.2.0 | `public class View implements Relation` |

### Spark 4.2 Requires `RelationCatalog`

A catalog implementing `TableCatalog` and `ViewCatalog` separately is rejected at load:

```
Catalog 'ice' implements both TableCatalog and ViewCatalog directly. Catalogs that expose both
tables and views must implement RelationCatalog instead, which centralizes the cross-cutting rules
(shared identifier namespace, cross-type collision rejection, single-RPC perf entry points).
   -- Catalogs$.validateRelationCatalog(Catalogs.scala:121)
```

`RelationCatalog extends TableCatalog, ViewCatalog` and adds one abstract method,
`loadRelation(Identifier)`.

### Local Build Results

Procedure: [`case_study/lakehouse_stack/BUILDING_ICEBERG_FOR_SPARK_4_2.md`](case_study/lakehouse_stack/BUILDING_ICEBERG_FOR_SPARK_4_2.md).
Build reproduces from scratch with `--no-build-cache`: 0 compile errors, 47 MB jar.

Verified with `case_study/lakehouse_stack/verify_iceberg_runtime.py` against a JDBC catalog on Spark
4.2.0:

| Capability | Result |
|---|---|
| `CREATE NAMESPACE` | pass |
| create table, read back | pass |
| `MERGE INTO` (requires session extensions) | pass |
| `CALL <catalog>.system.expire_snapshots` | pass |
| metadata table `.snapshots` | pass |
| `ALTER TABLE ... ADD COLUMN` | pass |
| time travel, `VERSION AS OF <literal>` | pass |
| row-level `DELETE FROM` | pass |
| `CREATE VIEW`, `SELECT`, `SHOW VIEWS`, `CREATE OR REPLACE`, `DROP VIEW` | pass |

Two configuration requirements are Iceberg's, not Spark's:

- **Views need `jdbc.schema-version=V1`** on a JDBC catalog. Without it, view operations fail with
  `JDBC catalog is initialized without view support`.
- **`HadoopCatalog` does not support views** in any Spark version. Views require `JdbcCatalog`,
  `HiveCatalog`, `NessieCatalog`, `InMemoryCatalog` or `RESTCatalog`.

### Session Extensions and the Startup Warning

A Connect server started with `--jars` and `spark.sql.extensions` logs a `ClassNotFoundException`
for the extension class at WARN level. It is expected and harmless: it applies to the server's
bootstrap session, built before `--jars` reach the classpath, while per-client sessions load the
extensions normally. Verified with `--jars` alone: `MERGE INTO` and `CALL <catalog>.system.*` both
succeed. Adding `--driver-class-path` does not change the warning either way.

Verify extensions by exercising them, not by reading the log.

### Metric Views over Iceberg

Spark 4.2 metric views cannot be created **inside** an Iceberg catalog, but can be created in the
default catalog with an Iceberg table as their source:

| | |
|---|---|
| Metric view inside the Iceberg catalog | fails with `MISSING_CATALOG_ABILITY.VIEWS` |
| Metric view in the default catalog, `source:` an Iceberg table | works |

Metric views themselves are confirmed working on Spark 4.2.0 GA.

---

## 5. Case Study: lakehouse-stack Migration Parity

The case study in `case_study/lakehouse_stack/` migrates
`~/lakehouse-stack/scripts/pipelines/pipeline_spark41.py` (357 lines). The pipeline was run
twice against the same source data: once on a classic driver via `spark-submit`, once over Spark
Connect with the migrated file. The two runs wrote to separate Iceberg warehouses registered under
different catalog names on one server, so a single session could compare them.

`parity.py` compares each table on three axes: schema fingerprint, row count, and an
order-independent content hash (`xxhash64` per row, combined with `bit_xor`). Row counts alone would
miss the failure worth catching: same row count, different values.

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

Zero transformation lines changed between the two files. The compatibility audit over the wider
`scripts/` tree reports 27 findings in 24 files: 21 blockers, all the same `sparkContext.setLogLevel`
idiom; 5 performance findings; 1 behavior finding (a temp view registered twice, which is a
correctness issue only under Connect).

## 6. Environment

| | |
|---|---|
| Spark | 4.2.0, git revision `32f72996011` |
| Images | `spark-connect-demo/spark:4.2.0`, built by `Dockerfile` from `apache/spark:4.2.0-scala2.13-java21-python3-ubuntu` with `pyarrow` 25.0.1 and `pandas` 2.3.3; `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu` for §8 |
| Cluster | `compose.yaml`: a standalone master, one worker with 2 cores and 2 GB, and a Connect server with `spark.cores.max=1` |
| Client | `pyspark-client==4.2.0` (`.venv`) and `pyspark[connect]==4.2.0` (`.venv-full`) on CPython 3.10.20, installed with uv; CPython 3.12.3 for §3 |
| JDK on the client machine | OpenJDK 17.0.20 |
| Iceberg | local build, no upstream release for Spark 4.2 |

Spark 4.2.0 contains no geospatial support: scanning every jar in the official image finds no
`GeographyVal`, no `GeometryVal`, no `TimestampNanosVal` and no `ST_*` functions. Secondary sources
stating otherwise are incorrect.

## 7. JIRA Metadata Notes

Several JIRA issues cited in this material have metadata that differs from how the features are
commonly described. Each was checked against issues.apache.org.

| JIRA | Note |
|---|---|
| SPARK-39375 | SPIP: Spark Connect. No fix version; the issue is Reopened, although the feature shipped in 3.4.0 |
| SPARK-42938 | Structured Streaming with Spark Connect. No fix version; Open. Listed in the 3.5.0 release notes |
| SPARK-42471 | PyTorch integration with Spark Connect. No fix version; Open. Listed in the 3.5.0 release notes |
| SPARK-42497 | pandas API on Spark for Spark Connect. No fix version; Resolved. Listed in the 3.5.0 release notes |
| SPARK-53484 | JDBC driver for Spark Connect. Fix version 4.1.0, not 4.2.0 |
| SPARK-54314 | Client code locations for telemetry. Resolved with no fix version set |
| SPARK-56007 | Titled "Row with duplicate column names throws error"; the release notes describe the fix as `ArrowDeserializer` positional binding |
| SPARK-57601 | Connect tab in the History Server. A bug fix, backported to 4.1.3 and 4.0.4 |

The remaining JIRA references, and the PyPI and Maven Central figures, match their sources.

## 8. Mixed Client and Server Versions

Recorded on 2026-09-10 with `make matrix`, which runs `extras/version_matrix/matrix.py` once per
client and server pairing, twice over. Each run performs the same fourteen operations and writes
`extras/version_matrix/results/<cell>-run<N>.json`. Both runs gave identical results in every cell.

The operations are a sample chosen to touch the main RPCs and the client methods added in 4.2. They
are not a compatibility matrix, and an operation absent from Table 8-2 was not tested.

*Table 8-1. Clients and servers*

| | |
|---|---|
| Client 4.2.0 | `pyspark-client==4.2.0`, CPython 3.10.20 (`.venv`) |
| Client 4.1.2 | `pyspark-client==4.1.2`, CPython 3.10.20 (`.venv-41`) |
| Server 4.2.0 | service `spark-connect`: image `spark-connect-demo/spark:4.2.0` (`sha256:d757475d0175…`), built from `apache/spark:4.2.0-scala2.13-java21-python3-ubuntu` (`sha256:ce89e23992aa…`) plus `pyarrow` 25.0.1 and `pandas` 2.3.3; runs on the standalone master and worker |
| Server 4.1.2 | service `spark-connect-41` (profile `versions`): `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu` (`sha256:bfbb0784386f…`), unmodified; default master, so execution runs inside the server process |
| Executor Python | 3.10.12 in both images |

The two servers differ in where they execute (a standalone cluster and the server process). None of
the fourteen operations depends on that difference.

*Table 8-2. Results by cell*

| Operation | A: client 4.2.0, server 4.2.0 | B: client 4.2.0, server 4.1.2 | C: client 4.1.2, server 4.2.0 | D: client 4.1.2, server 4.1.2 |
|---|---|---|---|---|
| connect, `spark.version` | 4.2.0 | 4.1.2 | 4.2.0 | 4.1.2 |
| `spark.sql("SELECT 1 AS one")` | ok | ok | ok | ok |
| `createDataFrame()` from a three-row list | ok | fails (1) | ok | ok |
| `groupBy().agg()` over `spark.range(10)` | ok | ok | ok | ok |
| `toPandas()` of 100,000 rows | ok | ok | ok | ok |
| `observe()` row count | 25 | 25 | 25 | 25 |
| Python UDF returning the executor's Python version | 3.10.12 | 3.10.12 | 3.10.12 | 3.10.12 |
| UDF fails, `addArtifact(geo.py, pyfile=True)`, UDF runs | 111.2 km | 111.2 km | 111.2 km | 111.2 km |
| `zipWithIndex()` | ok | ok | not in client | not in client |
| `read.json()` given a DataFrame built with `spark.sql()` | ok | ok | fails (2) | fails (2) |
| `emptyDataFrame("a int")` | ok | ok | not in client | not in client |
| `GetStatus` through the private `client._get_operation_statuses()` | ok | fails (3) | not in client | not in client |
| unresolved column: error class, SQL state | `UNRESOLVED_COLUMN.WITH_SUGGESTION`, 42703 | same | same | same |
| `interruptAll()` | ok | ok | ok | ok |

First lines of the errors, as recorded:

1. `ValueError: invalid literal for int() with base 10: '3221225472b'`
2. `AssertionError` (no message)
3. `SparkConnectGrpcException: status = StatusCode.UNIMPLEMENTED`

"Not in client" means the 4.1.2 client has no such method, checked with `hasattr` before the call.

Apart from the reported server version, cell C's results are identical to cell D's, and the
`read.json()` failure occurs against both servers, so it belongs to the 4.1.2 client. Cell B differs
from cell A in the two failed operations only.

### The `createDataFrame()` Failure

The 4.2.0 client (`pyspark/sql/connect/session.py`, lines 767 to 781) converts five
`spark.sql.session.localRelation*` settings from the server with `int()` before it compares the data
against the cache threshold, so the conversion runs for local data of any size. The 4.1.2 client
reads three of those settings and not `spark.sql.session.localRelationSizeLimit`. The setting was
added to the client by [SPARK-55047](https://issues.apache.org/jira/browse/SPARK-55047) ("Add
client-side limit for local relation size", fix version 4.2.0).

The values returned by each server for that setting:

| Server | `spark.sql.session.localRelationSizeLimit` |
|---|---|
| 4.2.0 | `3221225472` |
| 4.1.2 | `3221225472b` |

The other four settings returned the same values from both servers. A JIRA text search for
`localRelationSizeLimit` on 2026-09-10 returned SPARK-55047 and SPARK-59224; neither mentions older
servers.

The first version of the matrix built the `read.json()` input with `createDataFrame()`, which made
`read.json()` appear to fail in cell B. The input now comes from `spark.sql()`, and the results
above are from that version.

## 9. Demonstration Pipeline, Examples and Tests

Recorded on 2026-09-10 against the cluster in `compose.yaml`, with `CONNECT_PORT=15095`.

**Input.** `pipeline/input/generate.py`, seeded with `random.Random(42)`, writes 238 order events
for 60 orders (5 of them undelivered, and 3 events without an order ID), 4 stores and 5 brands.
Regenerating the files produces byte-identical output (compared by SHA-256).

**Pipeline.** `make pipeline` runs `pipeline_before.py` with `spark-submit` inside the Connect
server's container (Spark Classic, standalone master, `spark.cores.max=1`), then
`pipeline_after.py` from the host over Spark Connect, then `parity.py`. It completed in 16 seconds.

*Table 9-1. Rows written by each run*

| Table | Spark Classic | Spark Connect |
|---|---|---|
| `bronze_events` | 238 | 238 |
| `bronze_stores` | 4 | 4 |
| `bronze_brands` | 5 | 5 |
| `silver_orders_enriched` | 235 | 235 |
| `silver_order_lifecycle` | 55 | 55 |
| `gold_delivery_performance` | 4 | 4 |
| `gold_brand_summary` | 5 | 5 |

`parity.py` reported `7/7 tables identical` on four runs. Comparing run 1 with run 3 of the same
mode also reported 7 of 7 identical, for Spark Classic and for Spark Connect.

With `PIPELINE_INPUT` set to the host path of `pipeline/input`, `pipeline_after.py` failed with
`AnalysisException: [PATH_NOT_FOUND] Path does not exist: file:/<project>/pipeline/input/order_events.jsonl. SQLSTATE: 42K03`.

**Audit.** `make audit` reports 6 findings (3 BLOCKER, 3 BEHAVIOR): in `pipeline_before.py`,
`setLogLevel()` (BLOCKER/L5, line 26), `addPyFile()` (BLOCKER/L4, line 27), `.rdd` (BLOCKER/L1,
line 30) and `enableHiveSupport()` (BEHAVIOR/L3, reported at line 21); none in `pipeline_after.py`;
and two BEHAVIOR/L1 findings in `examples/10_eager_and_lazy.py`.

**Diff.** `git diff --no-index --stat pipeline/pipeline_before.py pipeline/pipeline_after.py`
reports 21 insertions and 13 deletions.

**Examples.** `make examples` exited with status 0 in 22 seconds, and `make doctor` reported all
checks passed. Example 03 reported `pyspark/` as 17 MB (sum of file sizes) in `.venv` and 500 MB in
`.venv-full`, with 276 JAR files totaling 477 MB.

*Table 9-2. Test results*

| Tier | Environment | Cluster running | Cluster stopped |
|---|---|---|---|
| `tests/unit` | `.venv` | 38 passed | 38 passed |
| `tests/spark` | `.venv-full`, Spark Classic `local[2]` | 16 passed | 16 passed |
| `tests/spark` and `tests/connect` | `.venv`, Spark Connect | 37 passed | 37 skipped |

`tests/unit` imports no PySpark module (checked with `python -X importtime`).

**Clean copy.** The project was copied without its virtual environments, `.env` or caches, the
cluster's volume was removed, and `.env` was created from `.env.example` with `CONNECT_PORT=15095` and
`MASTER_UI_PORT=8184`, because ports 15002 and 8080 are in use on the test machine. The uv and Docker
caches were warm, so the setup and startup times do not describe a first installation.

*Table 9-3. Targets run in the clean copy*

| Target | Exit status | Time |
|---|---|---|
| `make setup` | 0 | 1 s |
| `make up` | 0 | 7 s |
| `make examples` | 0 | 24 s |
| `make doctor` | 0 | 2 s |
| `make audit` | 0 | under 1 s |
| `make pipeline` | 0 | 16 s |
| `make test` | 0 | 14 s |
| `make check` | 0 | under 1 s |

`make check` does not follow links that lead outside the project, such as links to other guides in
the series, because a copy of the project alone cannot resolve them.
