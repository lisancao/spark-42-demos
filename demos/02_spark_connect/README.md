# Demo 2: Spark Connect on Apache Spark 4.2

Use this project to run a Spark Connect client/server setup and migrate a small DataFrame pipeline
without changing its output. The [blog post](blog_spark_connect.md) and
[video script](video_spark_connect.md) explain the same two demos:

- **Demo A: Setup.** Ten short examples, each showing one behavior of Spark Connect, and a `doctor`
  command that checks a client and a server.
- **Demo B: Migration.** A small pipeline migrated from Spark Classic to Spark Connect, with one
  change for each migration layer and a comparison showing that the output is unchanged.

The migration of a 357-line pipeline copied from the `lakehouse-stack` project is included as a written case study, and
[`setup/TOPOLOGIES.md`](setup/TOPOLOGIES.md) describes where a Connect server can run.

Everything here was verified against Apache Spark **4.2.0** (released 2026-07-14) on 2026-09-09 and
2026-09-10; "Verification Status", below, lists what was run.

## About Spark Connect

Spark Connect, introduced in Spark 3.4, separates a client application from the Spark driver. The
client builds a DataFrame plan and sends it over gRPC; the server resolves, optimizes and executes
it, and returns results as Arrow. The session lives on the server, and the client holds a reference
to it. Why Spark adopted this design is described in §1 of the blog post.

```text
Python application
  └── SparkSession.remote(...)
      └── DataFrame operations build an unresolved plan
          └── gRPC sends the plan to the Connect server
              └── Spark resolves, optimizes, and executes it
                  └── Arrow returns result batches to the client
```

For an application, session creation changes:

```python
spark = SparkSession.builder.remote("sc://host:15002").getOrCreate()
```

*Table 1. What an application gains*

| Benefit | Description |
|---|---|
| No JVM on the client | No JDK to install and no Spark JARs (456 MB in the full distribution). A client failure does not stop the driver. |
| Credentials held by the server | Catalog passwords and object-storage keys are server configuration, rather than files mounted on every node that runs application code. |
| Independent upgrades | The server can be upgraded without redeploying client applications. No compatibility matrix is published; blog post §2 records a sample of mixed-version results. |
| Embeddability | A notebook, web service or agent can hold a Spark session without a Spark distribution. |

Two limitations apply:

- **`SparkContext` is unavailable**, together with everything reached through it: `setLogLevel`,
  `addPyFile`, broadcast variables, accumulators and RDDs. In the case study's repository, 21 of the
  audit's 27 findings were a single call (`setLogLevel`).
- **Python versions remain coupled.** A Python UDF is serialized on the client and deserialized on an
  executor, so client and executor Python minor versions must match.

## Requirements

*Table 2. Requirements*

| Requirement | Used for |
|---|---|
| Docker with Compose v2 | The Spark cluster (`make up`) |
| [uv](https://docs.astral.sh/uv/) | Python 3.10 and the two virtual environments (`make setup`) |
| GNU Make | The targets below |
| Java 17 or 21 on the host | Only the Spark Classic runs: examples 01 and 10 in `classic` mode, example 04, and the Spark Classic test tier |

## Quick Start

```bash
make setup      # .venv with pyspark-client and .venv-full with pyspark[connect], on Python 3.10
make up         # build the image; start a Spark master, a worker and a Connect server
make examples   # run the ten examples in order
make doctor     # check the client, the endpoint, the server and its capabilities
make pipeline   # Demo B: run the pipeline on Spark Classic and on Spark Connect, then compare
make test       # unit tests, then Spark tests on Spark Classic and on Spark Connect
make down       # stop the cluster
```

`make help` lists every target. On the test machine, `make examples` took 22 seconds and
`make pipeline` 16 seconds.

The ports default to those in Table 3. To change them, copy `.env.example` to `.env` and edit it,
keeping `SPARK_REMOTE` consistent with `CONNECT_PORT`.

### Opening the Project in an IDE

Open this directory as the workspace root. `.vscode/launch.json` provides 18 run configurations,
numbered in the order the video uses them and each with the interpreter it needs, plus three test
configurations. The configurations read `.env`.

## Services and Ports

`compose.yaml` builds `spark-connect-demo/spark:4.2.0` from the `Dockerfile`: the official
`apache/spark:4.2.0-scala2.13-java21-python3-ubuntu` image with `pyarrow` 25.0.1 and `pandas` 2.3.3
added. The official image does not include PyArrow, and Spark 4.2 runs Python UDFs through Arrow by
default, so with default settings every Python UDF fails on the unmodified image (blog post §7).

*Table 3. Services*

| Service | Port on 127.0.0.1 | Setting in `.env` | Purpose |
|---|---|---|---|
| `spark-master` | 8080 | `MASTER_UI_PORT` | Standalone master web UI |
| `spark-worker` | none | | One worker with 2 cores and 2 GB of memory |
| `spark-connect` | 15002 | `CONNECT_PORT` | Spark Connect server (gRPC); runs its applications on the cluster with one core |
| `spark-connect` | 4040 | `CONNECT_UI_PORT` | The Connect server's application UI |
| `spark-connect-41` | 15096 | `CONNECT_41_PORT` | A Spark 4.1.2 Connect server, started only by `make matrix` (profile `versions`) |

The worker and the Connect server share a volume at `/opt/spark/work-dir`, where the pipeline writes
its output, and mount `pipeline/` read-only at `/demo/pipeline`.

## Demo A: Examples

Each example is a standalone script that imports only the standard library and PySpark. Unless
Table 4 says otherwise, an example runs in `.venv` (`pyspark-client`) and connects to
`SPARK_REMOTE`.

*Table 4. Examples in `examples/`*

| File | Shows | Environment | Blog post | Launch configurations |
|---|---|---|---|---|
| `01_classic_or_connect.py` | The same DataFrame function on Spark Classic and on Spark Connect | `.venv-full` for `classic`, `.venv` for `connect` | §4, §6 | 1, 2 |
| `02_first_query.py` | A query runs on the server; `explain()` prints the server's plan | | §2 | 3 |
| `03_which_client.py` | Identifying `pyspark-client` by its structure: no `jars/` and no Py4J | both | §3 | 4, 5 |
| `04_in_process.py` | `remote("local[*]")` starts a server inside the Python process | `.venv-full` | §4; TOPOLOGIES §1 | 6, 7 |
| `05_sessions.py` | `getOrCreate()` reuses a session for a different URL; `create()` isolates sessions | | §7 | 8 |
| `06_python_versions.py` | Client and executor Python versions | | §7 | 10 |
| `07_connection_strings.py` | A `/` in a parameter value is rejected | | §5 | 11 |
| `08_add_artifact.py` | A UDF fails until `addArtifact()` ships its module | | §9, Layer 4 | 12 |
| `09_api_differences.py` | Attributes that need the JVM, `observe()`, and methods added in 4.2 | | §7 | 13 |
| `10_eager_and_lazy.py` | When an invalid column raises, and temp views resolved by name | `.venv-full` for `classic`, `.venv` for `connect` | §8 | 15, 16 |

Launch configuration 9 runs `tools/doctor.py`, 14 the audit, 17 the pipeline on Spark Connect, and
18 the parity check.

Example 10 prints a different result in each mode:

```
== 10 classic
mode: classic
invalid column: error raised when the DataFrame is defined
view t replaced: the earlier DataFrame now counts 3 rows
== 10 connect
mode: connect
invalid column: no error at definition; error raised when the action runs
view t replaced: the earlier DataFrame now counts 7 rows
```

`make doctor` checks the client, the endpoint, the server version and the server's capabilities, and
exits with status 0 only if every check passes. Output on the test machine, where `CONNECT_PORT` was
15095:

```
client
       distribution           pyspark-client 4.2.0
       jars                   none
       py4j                   absent
       python                 3.10.20
endpoint
       url                    sc://localhost:15095
  ok   reachable
server
  ok   version                4.2.0
capabilities
  ok   sql                    1
  ok   arrow collect          1000 rows
  ok   observe                10
  ok   python versions        both 3.10
  ok   add artifact           fails before addArtifact, succeeds after
server configuration (values of secret-like keys are masked)
       spark.api.mode                                 <unset>
       spark.sql.execution.arrow.pyspark.enabled      true
       spark.sql.execution.pythonUDF.arrow.enabled    true
       spark.sql.shuffle.partitions                   200
all checks passed
```

## Demo B: Migrating a Pipeline

`pipeline/pipeline_before.py` runs a seven-table pipeline on Spark Classic with `spark-submit`, and
`pipeline/pipeline_after.py` runs it over Spark Connect from the host. The two files differ in one
place for each migration layer, and both call the transformations in `pipeline/lib/transforms.py`.
[`pipeline/MIGRATION.md`](pipeline/MIGRATION.md) walks through each change.

```bash
make audit      # 4 findings in pipeline_before.py, none in pipeline_after.py
make pipeline   # both runs, then the comparison
```

`make pipeline` ends with the comparison:

```
parity: /opt/spark/work-dir/out/classic  vs  /opt/spark/work-dir/out/connect
  bronze_events                ok       238 rows, hashes match
  bronze_stores                ok       4 rows, hashes match
  bronze_brands                ok       5 rows, hashes match
  silver_orders_enriched       ok       235 rows, hashes match
  silver_order_lifecycle       ok       55 rows, hashes match
  gold_delivery_performance    ok       4 rows, hashes match
  gold_brand_summary           ok       5 rows, hashes match
  7/7 tables identical
```

## Tests

*Table 5. Test tiers*

| Directory | Needs | Tests | `make test` runs it with |
|---|---|---|---|
| `tests/unit` | Nothing; Spark is not imported | 38 | `.venv` |
| `tests/spark` | Spark Classic or a Connect server | 16 | `.venv-full` on Spark Classic, then `.venv` on Spark Connect |
| `tests/connect` | A Connect server | 21 | `.venv` |

The `spark` tier runs the same tests in both modes: its session fixture uses Spark Connect when
`SPARK_REMOTE` is set and a local Spark Classic session otherwise. When the server cannot be
reached, the Connect runs are skipped; with the cluster stopped, `make test` reported 38 passed,
16 passed and 37 skipped.

## Project layout

```text
02_spark_connect/
├── examples/          # 10 focused client and server behaviors
├── pipeline/          # Classic/Connect pair, shared transforms, and parity check
├── case_study/        # Migration of the lakehouse-stack pipeline
├── tests/             # Unit, Spark parity, and Connect server checks
├── tools/             # Diagnose, audit compatibility, and check documents
├── extras/            # Version matrix, benchmarks, and Rust/Polars client
├── setup/             # Standalone and Kubernetes server examples
├── compose.yaml       # Master, worker, and Connect services
└── blog_spark_connect.md  # Architecture and adoption guide
```

## Case Study

`case_study/lakehouse_stack/` describes the migration of a 357-line pipeline copied from the
`lakehouse-stack` project that writes
10 Iceberg tables; parity was verified for all 10 tables, including 1,027,129 bronze orders. It
requires `~/lakehouse-stack` data and a locally built Iceberg runtime, because Apache Iceberg has no
Spark 4.2 release, and it has its own Makefile.

`setup/` contains `TOPOLOGIES.md`, a script that starts a server from a Spark distribution, and
Kubernetes manifests, which are untested here.

## Extras

The demos do not use `extras/`. It holds two measurements and their recorded runs, so
that the figures can be rerun, and the Rust client example from blog post §12.

- **`extras/bench/`: what the client and server split costs.** `protocol_overhead.py` times the same
  work on Spark Classic and on Spark Connect on one machine: starting a session, small queries,
  reading a schema, collecting results and aggregating. `shuffle_partitions.py` repeats one
  aggregation at 8, 32, 200 and 800 shuffle partitions, because Connect's overhead grows with the
  partition count. The JSON files are the recorded runs, made with CPython 3.12.3 before the project
  was pinned to Python 3.10. There is no `make` target; each script's docstring shows how to run
  it.
- **`extras/version_matrix/`: whether a client and server of different versions work together.**
  `make matrix` starts a Spark 4.1.2 Connect server beside the 4.2.0 one, runs the same fourteen
  operations for each pairing of `pyspark-client` 4.1.2 or 4.2.0 with either server, twice, and
  prints a table. `results/` holds each run's output, including the first line of every error. The
  summary, and the one surprising failure (a 4.2.0 client cannot create a DataFrame from local Python
  data on a 4.1.2 server), are in blog post §2, Table 2-1.
- **`extras/rust_polars/`: the Spark Connect Rust client with Polars.** A Cargo project that runs an
  aggregation on the Connect server with the `apache-spark-connect` 4.2.0 crate and continues in
  Polars, and `extras/rust_polars/examples/zip_check.rs`, which builds a plan that a 4.2.0 server
  rejects. It needs Rust 1.95 or later and `protoc`; blog post §12 describes the build. Run it from
  that directory with `SPARK_REMOTE=sc://localhost:15002 cargo run --release`.

## Verification Status

On 2026-09-10, against the cluster in `compose.yaml`:

- `make examples` exited with status 0, and `make doctor` reported all checks passed.
- `make pipeline` reported 7 of 7 tables identical on four runs.
- `make test` passed 38 unit tests, 16 Spark tests on Spark Classic, and 37 tests on Spark Connect.
- `make matrix` gave identical results on both runs in every cell.
- On 2026-09-11, the release build of `extras/rust_polars/` (Rust 1.98.1) printed the output shown in
  blog post §12 on four runs, and `zip_check` reproduced the rejected plan.
- `ruff check .` and Pyright reported no issues.
- A copy of the project without virtual environments or `.env`, started from an empty cluster
  volume, ran `make setup`, `up`, `examples`, `doctor`, `audit`, `pipeline`, `test` and `check`, each
  exiting with status 0, in about a minute with warm uv and Docker caches.
- All 14 graphics, in both themes, passed checks for the shared palette, text contrast of at least
  4.5:1, a 20 px minimum text size and a 1920 by 1080 canvas, and were rendered in cairosvg and
  Chrome.

## Known Constraints

- **The client is pinned to Python 3.10.** The official Spark 4.2.0 image ships Python 3.10.12, and
  Python UDFs require the same minor version on the client and the executors.
- **Example 04 needs port 15002.** In-process mode always connects to `localhost:15002`. With the
  default `CONNECT_PORT`, the `spark-connect` service holds that port, so the example exits with an
  explanation rather than connecting to that server. Run it before `make up`, or set `CONNECT_PORT`
  to another port in `.env`.
- **An unreachable endpoint can block without output.** With nothing listening at the address,
  `SparkSession.builder.remote(url).create()` had not returned after about 18 minutes, when it was
  stopped. Run `make doctor` first; it tests the address with a three-second timeout.
- **A 4.2.0 client cannot create a DataFrame from local Python data on a 4.1.2 server.** The failure
  and its cause are described in blog post §2.
- **The Kubernetes manifests in `setup/kubernetes/` are untested.**
