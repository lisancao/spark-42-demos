# Demo 1: Metric Views in Apache Spark 4.2

This project accompanies the [blog post](blog_metric_views.md) and two video scripts: a long-form
video ([`video_metric_views.md`](video_metric_views.md)) and a short reel
([`video_metric_views_reel.md`](video_metric_views_reel.md)). It contains eight short examples, each
showing one behavior of metric views, a `doctor` command that checks the setup, and a probe that
records how Spark treats metric view definitions, queries and catalog commands.

Everything here was verified against Apache Spark **4.2.0** (released 2026-07-14) on 2026-09-11;
"Verification Status", below, lists what was run.

## About Metric Views

A metric view, added in Spark 4.2 (SPARK-54119), is a view whose definition is a YAML document of
dimensions and measures. A measure is an aggregate expression written without a grouping. A query
asks for it with `MEASURE()` and supplies the grouping, and Spark evaluates the measure's formula at
that grain. Why Spark added metric views is described in §1 of the blog post.

```sql
CREATE VIEW delivery_metrics WITH METRICS LANGUAGE YAML AS $$ ... $$;
SELECT region, MEASURE(conversion_rate) FROM delivery_metrics GROUP BY region;
```

*Table 1. Two questions about the demonstration data*

| Question | Written by hand | Result | With `MEASURE()` |
|---|---|---|---|
| Conversion rate for June | Average of the five regional rates | 0.2384 | 0.0923 |
| Users active in June | Sum of the daily distinct counts | 69,420 | 9,994 |

Two limits apply:

- **A metric view does not stop a second aggregation.** A query that sums daily
  `MEASURE(active_users)` results gets 69,420 again (blog post §6).
- **Spark 4.2.0 has no documentation page for metric views.** The YAML fields, query clauses and
  catalog commands in blog post §4, §5 and §7 were measured with `grammar_probe/probe.py`.

## Requirements

*Table 2. Requirements*

| Requirement | Used for |
|---|---|
| Docker with Compose v2 | The Spark Connect server (`make up`) |
| [uv](https://docs.astral.sh/uv/) | Python 3.10 and the two virtual environments (`make setup`) |
| GNU Make | The targets below |
| Java on the host (17.0.20 was used) | Only the Spark Classic runs: the Spark Classic test tier and the probe's Classic run |

## Quick Start

```bash
make setup      # .venv with pyspark-client and .venv-full with pyspark[connect], on Python 3.10
make data       # generate the dataset in data/
make up         # start the Spark Connect server
make doctor     # check the client, the server, the mounted dataset and a metric view
make examples   # run the eight examples in order
make test       # unit tests, then Spark tests on Spark Classic and on Spark Connect
make down       # stop the server
```

`make help` lists every target. `make reel` runs examples 01 to 05, the ones the short video uses.

The ports default to those in Table 3. To change them, copy `.env.example` to `.env` and edit it,
keeping `SPARK_REMOTE` consistent with `CONNECT_PORT`.

### Opening the Project in an IDE

Open this directory as the workspace root. `.vscode/launch.json` provides 10 run configurations,
numbered in the order the long-form video uses them and each with the interpreter it needs, plus a
configuration that compares the recorded probe runs and three test configurations. The configurations
read `.env`.

## Services and Ports

`compose.yaml` starts one service, `spark-connect`, from the official
`apache/spark:4.2.0-scala2.13-java21-python3-ubuntu` image with nothing added. The examples run no
Python UDFs, so the server needs no Python packages.

*Table 3. Services*

| Service | Port on 127.0.0.1 | Setting in `.env` | Purpose |
|---|---|---|---|
| `spark-connect` | 15002 | `CONNECT_PORT` | Spark Connect server (gRPC) |
| `spark-connect` | 4040 | `CONNECT_UI_PORT` | The server's application UI |

`data/` is mounted read-only at `/demo/data`, and the examples register its Parquet files as external
tables. The warehouse directory, `/tmp/warehouse`, is a tmpfs, so anything written to it is removed
when the container stops.

## Examples

Each example is a standalone script that imports only the standard library and PySpark, runs in
`.venv` (`pyspark-client`), and connects to `SPARK_REMOTE`.

*Table 4. Examples in `examples/`*

| File | Shows | Blog post | Launch configuration |
|---|---|---|---|
| `01_average_of_ratios.py` | The average of regional conversion rates and the rate from the totals | §3 | 3 |
| `02_summing_distinct_counts.py` | Daily distinct users added together, and distinct users for the month | §3 | 4 |
| `03_create_metric_view.py` | Creating `delivery_metrics` and describing it | §4 | 5 |
| `04_measure_at_every_grain.py` | `MEASURE()` over the whole month, by region and by day | §5 | 6 |
| `05_what_it_does_not_prevent.py` | Aggregating the results of `MEASURE()` a second time | §6 | 7 |
| `06_definition_rules.py` | Definitions and queries that Spark rejects, with their error conditions | §7 | 8 |
| `07_view_lifecycle.py` | Replacing, describing, renaming, altering and dropping a metric view | §7 | 9 |
| `08_modeled_source.py` | A metric view over a joined query, with a filter and derived dimensions | §8 | 10 |

Launch configuration 1 generates the dataset, and 2 runs `tools/doctor.py`. Examples 04 and 05 query
the view that example 03 creates.

Example 06 prints each definition or query and whether Spark accepted it:

```
Definitions
  accepted  the definition as written
  rejected  version: 1.0
            INVALID_METRIC_VIEW_YAML
  rejected  a joins key
            INVALID_METRIC_VIEW_YAML
  rejected  display_name on a dimension
            INVALID_METRIC_VIEW_YAML
  rejected  a temporary view as the source
            INVALID_TEMP_OBJ_REFERENCE
  rejected  a measure without an aggregate function
            MISSING_AGGREGATION
  accepted  a query as the source
Queries
  rejected  SELECT *
            INTERNAL_ERROR
  rejected  a measure without MEASURE()
            INTERNAL_ERROR
  rejected  a dimension without GROUP BY
            MISSING_GROUP_BY
  rejected  a source column that is not a dimension
            UNRESOLVED_COLUMN.WITH_SUGGESTION
  rejected  MEASURE() in ORDER BY
            UNRESOLVED_COLUMN.WITH_SUGGESTION
  accepted  the measure's alias in ORDER BY
```

`make doctor` checks the client, the endpoint, the server version, the mounted dataset and a metric
view, and exits with status 0 only if every check passes. Output on the test machine, where
`CONNECT_PORT` was 15094:

```
client
       distribution           pyspark-client 4.2.0
       jars                   none
       python                 3.10.20
endpoint
       url                    sc://localhost:15094
  ok   reachable
server
  ok   version                4.2.0
dataset and metric views
  ok   dataset                79,000 rows in /demo/data/sessions.parquet
  ok   metric view            created, queried with MEASURE() and dropped
server configuration (values of secret-like keys are masked)
       spark.sql.catalogImplementation                in-memory
       spark.sql.warehouse.dir                        file:/tmp/warehouse
       spark.sql.session.timeZone                     Etc/UTC
all checks passed
```

## Grammar Probe

Spark 4.2.0 has no documentation page for metric views, so the rules in blog post §7 were measured.
`grammar_probe/probe.py` runs 87 cases covering YAML fields, sources, expressions, statement clauses,
query clauses, the DataFrame API and lifecycle commands. For every statement, it records the rows
returned, or the error condition, SQLSTATE and first message line.

```bash
make probe      # two runs over Spark Connect, one on Spark Classic, then a comparison
```

`grammar_probe/results/` holds the three recorded runs. `make probe` fails if the two Spark Connect
runs differ, and `tests/connect/test_server.py` replays every case against the server and compares
the results with the first recorded run.

## Tests

*Table 5. Test tiers*

| Directory | Needs | Tests | `make test` runs it with |
|---|---|---|---|
| `tests/unit` | Nothing; Spark is not imported | 12 | `.venv` |
| `tests/spark` | Spark Classic or a Connect server | 6 | `.venv-full` on Spark Classic, then `.venv` on Spark Connect |
| `tests/connect` | A Connect server | 91 | `.venv` |

The unit tests recompute every figure in `tests/figures.py` from the generated data with pandas. The
`spark` tier compares `MEASURE()` results with hand-written aggregates on both Spark Classic and
Spark Connect. `make check` runs `tools/check.py`, which checks that the documents quote only numbers
in `tests/figures.py` and error conditions that the probe recorded.

## Project Layout

```
01_metrics_views/
├── examples/        Eight numbered scripts
├── grammar_probe/   probe.py, and results/ with the recorded runs
├── tests/           figures.py, and the unit/, spark/ and connect/ tiers
├── tools/           generate_data.py, doctor.py, check.py, teleprompter_export.py
├── graphics/        Video graphics as 1920 by 1080 SVG (light versions in graphics/light/); blog figures in graphics/blog/
├── data/            The generated dataset; not in version control
├── compose.yaml, Makefile, pyproject.toml, uv.lock
├── blog_metric_views.md, video_metric_views.md, video_metric_views_reel.md, and teleprompter exports
└── .vscode/         Run configurations in video order, tasks and settings
```

## Verification Status

On 2026-09-11, against the server in `compose.yaml`:

- `make examples` exited with status 0 on two runs with identical output, and `make doctor` reported
  all checks passed.
- `make probe` recorded 87 cases. The two Spark Connect runs were identical, and the Spark Classic
  run accepted and rejected the same statements with the same conditions.
- `make test` passed 12 unit tests, 6 Spark tests on Spark Classic, and 97 tests on Spark Connect.
- `ruff check .` and Pyright reported no issues.
- A copy of the project without virtual environments, `.env` or `data/`, with its own ports and
  Compose project name, ran `make setup`, `data`, `up`, `doctor`, `examples`, `reel`, `probe`, `test`
  and `teleprompter`, each exiting with status 0, in about a minute with warm uv and Docker caches.
  Its two Spark Connect probe runs were identical to the recorded runs.
- All 6 video graphics, in both themes, passed checks for the shared palette, text contrast of at
  least 4.5:1, a 20 px minimum text size and a 1920 by 1080 canvas, and were rendered in cairosvg.
- `make check` found no problems in this README, the blog post or the two video scripts.

## Known Constraints

- **Definitions last until the server stops.** The server uses Spark's in-memory catalog, so
  `make down` removes every table and metric view. Each example registers the tables it needs, and
  examples 04 and 05 need the view from example 03.
- **`CREATE OR REPLACE` does not replace a metric view in the session catalog.** It fails with
  `TABLE_OR_VIEW_ALREADY_EXISTS`, so the examples drop a view before creating it (blog post §7).
- **Some invalid statements fail with `INTERNAL_ERROR`,** including `SELECT *` from a metric view and
  `CREATE VIEW ... WITH METRICS` without a `LANGUAGE` clause (blog post §7).
- **The client is pinned to Python 3.10,** the version in the official Spark 4.2.0 image.
