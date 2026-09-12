"""Record how Apache Spark 4.2.0 treats metric view definitions, queries and lifecycle commands.

    python grammar_probe/probe.py --run connect-1       # over Spark Connect (SPARK_REMOTE)
    python grammar_probe/probe.py --run classic         # Spark Classic, in this process
    python grammar_probe/probe.py --report              # compare the recorded runs

Each case runs a few statements and records, for every statement, the rows it returned or the
error condition, SQLSTATE and first message line. Results are written to
grammar_probe/results/<run>.json. The blog post's statements about accepted and rejected
definitions come from these files (blog post §7).
"""
from __future__ import annotations

import argparse
import datetime
import importlib.metadata
import json
import logging
import os
import re
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = Path(__file__).resolve().parent / "results"
MAX_ROWS = 40
# Creation times, plan expression IDs and Py4J object IDs differ between runs, so they are
# replaced before results are saved.
TIME = re.compile(
    r"\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun) \w{3} \d{1,2} \d\d:\d\d:\d\d \w+ \d{4}\b"
    r"|\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(\.\d+)?Z?"
)
RUN_ID = re.compile(r"(#|\bo)\d+L?\b")
JAVA_CLASS = re.compile(r"^(?:[a-z_]\w*\.)+[A-Z]\w*(?:\$\w+)*: ")

DIMENSIONS = """dimensions:
  - name: region
    expr: region
  - name: session_date
    expr: session_date
"""
MEASURES = """measures:
  - name: total_sessions
    expr: COUNT(1)
  - name: conversion_rate
    expr: SUM(CAST(converted AS INT)) / COUNT(1)
  - name: active_users
    expr: COUNT(DISTINCT user_id)
"""
BASE_YAML = "version: 0.1\nsource: sessions\n" + DIMENSIONS + MEASURES
REGION_DIMENSION = "  - name: region\n    expr: region\n"
SESSIONS_MEASURE = "  - name: total_sessions\n    expr: COUNT(1)\n"
DAY_TYPE = (
    "  - name: day_type\n    expr: CASE WHEN dayofweek(to_date(session_date)) IN (1, 7)"
    " THEN 'weekend' ELSE 'weekday' END\n"
)
JOIN_SOURCE = (
    "SELECT s.session_id, s.user_id, s.region, s.converted, o.order_total"
    " FROM sessions s LEFT JOIN orders o ON s.session_id = o.session_id"
)
ORDERS_YAML = """version: 0.1
source: {source}
dimensions:
  - name: region
    expr: region
measures:
  - name: orders
    expr: COUNT(order_total)
  - name: revenue
    expr: SUM(order_total)
"""
JOINS = """joins:
  - name: users
    source: users
    on: source.user_id = users.user_id
"""


def create(name: str, yaml: str, head: str = "CREATE VIEW") -> str:
    return f"{head} {name} WITH METRICS LANGUAGE YAML AS $$\n{yaml}$$"


def with_source(source: str) -> str:
    return BASE_YAML.replace("source: sessions\n", f"source: {source}\n")


def after_region(text: str) -> str:
    return BASE_YAML.replace(REGION_DIMENSION, REGION_DIMENSION + text)


def after_sessions(text: str) -> str:
    return BASE_YAML.replace(SESSIONS_MEASURE, SESSIONS_MEASURE + text)


def build_cases(spark, data: str) -> list[dict]:
    from pyspark.sql import functions as F

    def case(case_id: str, group: str, *steps) -> dict:
        return {"id": case_id, "group": group, "steps": list(steps)}

    def view(case_id: str, yaml: str, head: str = "CREATE VIEW") -> str:
        return create(f"probe_{case_id}", yaml, head)

    def select(case_id: str, columns: str) -> str:
        return f"SELECT {columns} FROM probe_{case_id}"

    renamed = BASE_YAML.replace("active_users", "unique_users")
    body = f"AS $$\n{BASE_YAML}$$"
    return [
        # The YAML document
        case("version_0_1", "yaml", view("version_0_1", BASE_YAML)),
        case("version_1_0", "yaml", view("version_1_0", BASE_YAML.replace("0.1", "1.0"))),
        case("version_1_1", "yaml", view("version_1_1", BASE_YAML.replace("0.1", "1.1"))),
        case("version_missing", "yaml",
             view("version_missing", BASE_YAML.replace("version: 0.1\n", ""))),
        case("source_missing", "yaml",
             view("source_missing", BASE_YAML.replace("source: sessions\n", ""))),
        case("dimensions_missing", "yaml",
             view("dimensions_missing", "version: 0.1\nsource: sessions\n" + MEASURES),
             select("dimensions_missing", "MEASURE(total_sessions)")),
        case("measures_missing", "yaml",
             view("measures_missing", "version: 0.1\nsource: sessions\n" + DIMENSIONS)),
        case("yaml_malformed", "yaml", view("yaml_malformed", "version: 0.1\nsource: [sessions\n")),
        case("key_fields", "yaml", view("key_fields", BASE_YAML.replace("dimensions:", "fields:"))),
        case("key_joins", "yaml", view("key_joins", BASE_YAML + JOINS)),
        case("key_filter", "yaml", view("key_filter", BASE_YAML + "filter: region <> 'remote'\n"),
             select("key_filter", "MEASURE(total_sessions)")),
        case("key_comment", "yaml", view("key_comment", BASE_YAML + "comment: Delivery metrics\n")),
        case("key_materialization", "yaml", view(
            "key_materialization", BASE_YAML + "materialization:\n  schedule: every 6 hours\n")),
        case("column_display_name", "yaml",
             view("column_display_name", after_region("    display_name: Region\n"))),
        case("column_comment", "yaml",
             view("column_comment", after_region("    comment: Delivery region\n"))),
        case("column_synonyms", "yaml",
             view("column_synonyms", after_region("    synonyms: [area]\n"))),
        case("column_format", "yaml",
             view("column_format", after_sessions("    format:\n      type: number\n"))),
        case("column_window", "yaml", view("column_window", after_sessions(
            "    window:\n      - order: session_date\n        range: trailing 7 day\n"
            "        semiadditive: last\n"))),
        # The source
        case("source_temp_view", "source", view("source_temp_view", with_source("sessions_temp"))),
        case("source_qualified_name", "source",
             view("source_qualified_name", with_source("spark_catalog.default.sessions")),
             select("source_qualified_name", "MEASURE(total_sessions)")),
        case("source_query", "source",
             view("source_query", with_source("SELECT * FROM sessions WHERE region <> 'remote'")),
             select("source_query", "MEASURE(total_sessions)")),
        case("source_query_join", "source",
             view("source_query_join", ORDERS_YAML.format(source=JOIN_SOURCE)),
             select("source_query_join", "MEASURE(orders), ROUND(MEASURE(revenue), 2)")),
        case("source_metric_view", "source", view("source_metric_view", with_source("mv_base")),
             select("source_metric_view", "MEASURE(total_sessions)")),
        case("source_metric_view_count", "source",
             view("source_metric_view_count", "version: 0.1\nsource: mv_base\ndimensions:\n"
                  + REGION_DIMENSION + "measures:\n  - name: row_count\n    expr: COUNT(1)\n"),
             select("source_metric_view_count", "MEASURE(row_count)")),
        case("source_ctas", "source",
             f"CREATE TABLE probe_facts AS {JOIN_SOURCE}",
             view("source_ctas", ORDERS_YAML.format(source="probe_facts")),
             select("source_ctas", "MEASURE(orders), ROUND(MEASURE(revenue), 2)")),
        case("source_dropped", "source",
             f"CREATE TABLE probe_src USING parquet LOCATION '{data}/sessions.parquet'",
             view("source_dropped", with_source("probe_src")),
             select("source_dropped", "MEASURE(total_sessions)"),
             "DROP TABLE probe_src",
             select("source_dropped", "MEASURE(total_sessions)")),
        # Dimension and measure expressions
        case("measure_without_aggregate", "expression",
             view("measure_without_aggregate",
                  after_sessions("  - name: converted_flag\n    expr: CAST(converted AS INT)\n")),
             select("measure_without_aggregate", "MEASURE(converted_flag)")),
        case("measure_window_function", "expression",
             view("measure_window_function",
                  after_sessions("  - name: running\n    expr: SUM(COUNT(1)) OVER ()\n")),
             select("measure_window_function", "MEASURE(running)")),
        case("measure_references_measure", "expression",
             view("measure_references_measure",
                  after_sessions("  - name: doubled\n    expr: total_sessions * 2\n")),
             select("measure_references_measure", "MEASURE(doubled)")),
        case("measure_calls_measure", "expression",
             view("measure_calls_measure",
                  after_sessions("  - name: doubled\n    expr: MEASURE(total_sessions) * 2\n")),
             select("measure_calls_measure", "MEASURE(doubled)")),
        case("dimension_aggregate", "expression",
             view("dimension_aggregate", after_region("  - name: n\n    expr: COUNT(1)\n")),
             "SELECT n, MEASURE(total_sessions) FROM probe_dimension_aggregate GROUP BY n"),
        case("dimension_derived", "expression",
             view("dimension_derived", after_region(DAY_TYPE)),
             "SELECT day_type, MEASURE(total_sessions) FROM probe_dimension_derived"
             " GROUP BY day_type ORDER BY day_type"),
        case("duplicate_name", "expression",
             view("duplicate_name", after_region("  - name: total_sessions\n    expr: region\n"))),
        # The CREATE VIEW statement
        case("clause_comment", "statement",
             f"CREATE VIEW probe_clause_comment WITH METRICS LANGUAGE YAML"
             f" COMMENT 'Delivery metrics' {body}"),
        case("clause_no_language", "statement",
             f"CREATE VIEW probe_clause_no_language WITH METRICS {body}"),
        case("clause_column_list", "statement",
             f"CREATE VIEW probe_clause_column_list (region, total_sessions)"
             f" WITH METRICS LANGUAGE YAML {body}"),
        case("clause_tblproperties", "statement",
             f"CREATE VIEW probe_clause_tblproperties WITH METRICS LANGUAGE YAML"
             f" TBLPROPERTIES ('team' = 'growth') {body}"),
        case("clause_string_body", "statement",
             f"CREATE VIEW probe_clause_string_body WITH METRICS LANGUAGE YAML AS '{BASE_YAML}'"),
        case("clause_temporary", "statement",
             view("clause_temporary", BASE_YAML, "CREATE TEMPORARY VIEW"),
             select("clause_temporary", "MEASURE(total_sessions)")),
        case("clause_or_replace_new", "statement",
             view("clause_or_replace_new", BASE_YAML, "CREATE OR REPLACE VIEW"),
             select("clause_or_replace_new", "MEASURE(total_sessions)")),
        # Queries against mv_base
        case("select_measures", "query",
             "SELECT ROUND(MEASURE(conversion_rate), 4) AS conversion_rate,"
             " MEASURE(active_users) AS active_users, MEASURE(total_sessions) AS sessions"
             " FROM mv_base"),
        case("select_star", "query", "SELECT * FROM mv_base"),
        case("measure_without_measure_fn", "query", "SELECT total_sessions FROM mv_base"),
        case("dimension_without_group_by", "query",
             "SELECT region, MEASURE(total_sessions) FROM mv_base"),
        case("dimension_only", "query",
             "SELECT region FROM mv_base GROUP BY region ORDER BY region"),
        case("source_column_not_dimension", "query",
             "SELECT user_id, MEASURE(total_sessions) FROM mv_base GROUP BY user_id"),
        case("where_dimension", "query",
             "SELECT MEASURE(total_sessions) FROM mv_base WHERE region = 'metro'"),
        case("where_source_column", "query",
             "SELECT MEASURE(total_sessions) FROM mv_base WHERE converted"),
        case("where_measure", "query",
             "SELECT region FROM mv_base WHERE MEASURE(total_sessions) > 1000 GROUP BY region"),
        case("having_measure", "query",
             "SELECT region, MEASURE(total_sessions) AS sessions FROM mv_base GROUP BY region"
             " HAVING MEASURE(total_sessions) > 1000 ORDER BY region"),
        case("having_alias", "query",
             "SELECT region, MEASURE(total_sessions) AS sessions FROM mv_base GROUP BY region"
             " HAVING sessions > 1000 ORDER BY region"),
        case("order_by_alias", "query",
             "SELECT region, MEASURE(total_sessions) AS sessions FROM mv_base GROUP BY region"
             " ORDER BY sessions DESC"),
        case("order_by_selected_measure", "query",
             "SELECT region, MEASURE(total_sessions) FROM mv_base GROUP BY region"
             " ORDER BY MEASURE(total_sessions) DESC"),
        case("order_by_unselected_measure", "query",
             "SELECT region, MEASURE(conversion_rate) FROM mv_base GROUP BY region"
             " ORDER BY MEASURE(total_sessions) DESC"),
        case("order_by_ordinal", "query",
             "SELECT region, MEASURE(total_sessions) FROM mv_base GROUP BY region ORDER BY 2 DESC"),
        case("order_by_unselected_dimension", "query",
             "SELECT MEASURE(total_sessions) FROM mv_base GROUP BY region ORDER BY region"),
        case("group_by_all", "query",
             "SELECT region, MEASURE(total_sessions) FROM mv_base GROUP BY ALL ORDER BY region"),
        case("limit", "query",
             "SELECT session_date, MEASURE(active_users) FROM mv_base GROUP BY session_date"
             " ORDER BY session_date LIMIT 3"),
        case("measure_in_expression", "query",
             "SELECT ROUND(MEASURE(conversion_rate) * 100, 1) AS percent FROM mv_base"),
        case("sum_of_daily_measure", "query",
             "SELECT SUM(active_users) FROM (SELECT session_date, MEASURE(active_users)"
             " AS active_users FROM mv_base GROUP BY session_date)"),
        case("average_of_region_measure", "query",
             "SELECT ROUND(AVG(rate), 4) FROM (SELECT region, MEASURE(conversion_rate) AS rate"
             " FROM mv_base GROUP BY region)"),
        case("cte", "query",
             "WITH by_region AS (SELECT region, MEASURE(total_sessions) AS sessions FROM mv_base"
             " GROUP BY region) SELECT SUM(sessions) FROM by_region"),
        case("join_on_dimension", "query",
             "SELECT m.region, MEASURE(m.total_sessions) FROM mv_base m"
             " JOIN (SELECT 'metro' AS region) r ON m.region = r.region GROUP BY m.region"),
        case("aggregate_on_view", "query", "SELECT COUNT(*) FROM mv_base"),
        case("measure_on_table", "query", "SELECT MEASURE(converted) FROM sessions"),
        # The DataFrame API
        case("dataframe_columns", "dataframe",
             ("spark.table('mv_base').columns", lambda: spark.table("mv_base").columns)),
        case("dataframe_group_by", "dataframe",
             ("groupBy('region').agg(F.expr('measure(total_sessions)'))",
              lambda: spark.table("mv_base").groupBy("region")
              .agg(F.expr("measure(total_sessions)").alias("sessions")).orderBy("region").collect())),
        case("dataframe_select_expr", "dataframe",
             ("selectExpr('round(measure(conversion_rate), 4)')",
              lambda: spark.table("mv_base").selectExpr("round(measure(conversion_rate), 4)")
              .collect())),
        case("dataframe_where", "dataframe",
             ("where(\"region = 'metro'\").selectExpr('measure(total_sessions)')",
              lambda: spark.table("mv_base").where("region = 'metro'")
              .selectExpr("measure(total_sessions)").collect())),
        case("dataframe_count", "dataframe",
             ("spark.table('mv_base').count()", lambda: spark.table("mv_base").count())),
        # Lifecycle and catalog commands
        case("create_existing", "lifecycle",
             view("create_existing", BASE_YAML), view("create_existing", BASE_YAML)),
        case("or_replace_existing", "lifecycle",
             view("or_replace_existing", BASE_YAML),
             view("or_replace_existing", renamed, "CREATE OR REPLACE VIEW"),
             select("or_replace_existing", "MEASURE(unique_users)")),
        case("if_not_exists_existing", "lifecycle",
             view("if_not_exists_existing", BASE_YAML),
             view("if_not_exists_existing", renamed, "CREATE VIEW IF NOT EXISTS"),
             select("if_not_exists_existing", "MEASURE(active_users)")),
        case("drop_view", "lifecycle", view("drop_view", BASE_YAML),
             "DROP VIEW probe_drop_view", "SHOW VIEWS LIKE 'probe_drop_view'"),
        case("drop_table", "lifecycle", view("drop_table", BASE_YAML),
             "DROP TABLE probe_drop_table", "SHOW VIEWS LIKE 'probe_drop_table'"),
        case("alter_as_select", "lifecycle", view("alter_as_select", BASE_YAML),
             "ALTER VIEW probe_alter_as_select AS SELECT region FROM sessions",
             select("alter_as_select", "MEASURE(total_sessions)")),
        case("alter_with_metrics", "lifecycle", view("alter_with_metrics", BASE_YAML),
             f"ALTER VIEW probe_alter_with_metrics WITH METRICS LANGUAGE YAML AS $$\n{renamed}$$"),
        case("rename", "lifecycle", view("rename", BASE_YAML),
             "ALTER VIEW probe_rename RENAME TO probe_renamed",
             "SELECT MEASURE(total_sessions) FROM probe_renamed"),
        case("set_tblproperties", "lifecycle", view("set_tblproperties", BASE_YAML),
             "ALTER VIEW probe_set_tblproperties SET TBLPROPERTIES ('team' = 'growth')",
             "SHOW TBLPROPERTIES probe_set_tblproperties"),
        case("show_create_table", "lifecycle", view("show_create_table", BASE_YAML),
             "SHOW CREATE TABLE probe_show_create_table"),
        case("show_views_and_tables", "lifecycle", view("show_views_and_tables", BASE_YAML),
             "SHOW VIEWS LIKE 'probe_show_views*'", "SHOW TABLES LIKE 'probe_show_views*'"),
        case("show_columns", "lifecycle", view("show_columns", BASE_YAML),
             "SHOW COLUMNS IN probe_show_columns"),
        case("describe", "lifecycle", view("describe", BASE_YAML), "DESCRIBE TABLE probe_describe"),
        case("describe_extended", "lifecycle", view("describe_extended", BASE_YAML),
             "DESCRIBE TABLE EXTENDED probe_describe_extended"),
        case("describe_json", "lifecycle", view("describe_json", BASE_YAML),
             "DESCRIBE TABLE EXTENDED probe_describe_json AS JSON"),
        case("insert_into", "lifecycle", view("insert_into", BASE_YAML),
             "INSERT INTO probe_insert_into VALUES ('metro', '2026-06-01')"),
        case("catalog_list_tables", "lifecycle", view("catalog_list_tables", BASE_YAML),
             ("spark.catalog.listTables()", lambda: sorted(
                 [t.name, str(t.tableType), str(t.isTemporary)]
                 for t in spark.catalog.listTables()
                 if t.name in ("mv_base", "probe_catalog_list_tables", "sessions")))),
    ]


def scrub(text: str) -> str:
    return RUN_ID.sub(r"\1<id>", TIME.sub("<time>", text))


def normalize(value) -> list[list[str]]:
    if isinstance(value, list):
        return [[scrub(str(v)) for v in row] if isinstance(row, (list, tuple))
                else [scrub(str(row))] for row in value[:MAX_ROWS]]
    return [[scrub(str(value))]]


def call(target, method: str) -> str | None:
    try:
        value = getattr(target, method)()
    except Exception:  # the method is missing, or the exception carries no condition
        return None
    return None if value is None else str(value)


def describe_error(exc: BaseException) -> dict:
    # Spark Classic raises Py4JJavaError for JVM exceptions that PySpark does not convert. The
    # condition and message are then read from the Java exception.
    java = getattr(exc, "java_exception", None)
    source = exc if java is None else java
    condition = call(source, "getCondition") or call(source, "getErrorClass")
    text = str(exc) if java is None else str(java.toString())
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    first = (lines or [type(exc).__name__])[0]
    if java is not None:  # drop the Java class name, which Spark Connect does not include
        first = JAVA_CLASS.sub("", first)
    return {
        "ok": False,
        "condition": condition,
        "sqlstate": call(source, "getSqlState"),
        "message": scrub(first)[:300],
        "type": type(exc).__name__,
    }


def run_step(spark, step) -> dict:
    label, action = (step, None) if isinstance(step, str) else step
    try:
        value = spark.sql(label).collect() if action is None else action()
    except Exception as exc:  # every outcome is recorded, including failures
        return {"statement": label, **describe_error(exc)}
    rows = normalize(value)
    count = len(value) if isinstance(value, list) else 1
    return {"statement": label, "ok": True, "row_count": count, "rows": rows}


def drop_probe_objects(spark, keep_base: bool) -> int:
    names = [row[1] for row in spark.sql("SHOW TABLES").collect()
             if row[1].startswith("probe_") or (row[1] == "mv_base" and not keep_base)]
    for name in names:
        # A metric view accepts one of the two DROP forms; the other fails and is ignored.
        for statement in (f"DROP VIEW IF EXISTS {name}", f"DROP TABLE IF EXISTS {name}"):
            try:
                spark.sql(statement).collect()
            except Exception:
                pass
    return len(names)


def prepare(spark, data: str) -> list[dict]:
    """Register the tables, the temporary view and the metric view mv_base that the cases use."""
    setup = [
        run_step(spark, f"CREATE TABLE IF NOT EXISTS {table} USING parquet"
                        f" LOCATION '{data}/{table}.parquet'")
        for table in ("sessions", "orders", "users")
    ]
    spark.read.parquet(f"{data}/sessions.parquet").createOrReplaceTempView("sessions_temp")
    setup.append(run_step(spark, create("mv_base", BASE_YAML)))
    return setup


def client_distribution() -> str:
    for distribution in ("pyspark-client", "pyspark"):
        try:
            return f"{distribution} {importlib.metadata.version(distribution)}"
        except importlib.metadata.PackageNotFoundError:
            continue
    return "unknown"


def record(run: str, remote: str, data: str) -> int:
    from pyspark.sql import SparkSession

    # Failures are recorded in the results, so the client's error log would only repeat them.
    logging.getLogger("SQLQueryContextLogger").setLevel(logging.CRITICAL)
    if run == "classic":
        warehouse = tempfile.mkdtemp(prefix="probe-warehouse-")
        spark = (
            SparkSession.builder.master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", warehouse)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("OFF")
        data = str(ROOT / "data")
    else:
        spark = SparkSession.builder.remote(remote).create()

    dropped = drop_probe_objects(spark, keep_base=False)
    setup = prepare(spark, data)
    results = []
    for case in build_cases(spark, data):
        steps = [run_step(spark, step) for step in case["steps"]]
        results.append({"id": case["id"], "group": case["group"], "steps": steps})
        last = steps[-1]
        outcome = "ok" if last["ok"] else (last["condition"] or last["type"])
        print(f"  {case['id']:<32} {outcome}")
        drop_probe_objects(spark, keep_base=True)

    meta = {
        "run": run,
        "mode": "classic" if run == "classic" else "connect",
        "spark_version": spark.version,
        "catalog": spark.conf.get("spark.sql.catalogImplementation"),
        "client": client_distribution(),
        "python": ".".join(map(str, sys.version_info[:3])),
        "recorded": datetime.date.today().isoformat(),
        "objects_dropped_at_start": dropped,
    }
    drop_probe_objects(spark, keep_base=False)
    spark.stop()
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"{run}.json"
    text = json.dumps({"meta": meta, "setup": setup, "cases": results}, indent=1) + "\n"
    if run == "classic":  # the local data path depends on where the project is checked out
        text = text.replace(data, "<data>")
    path.write_text(text)
    print(f"wrote {path.relative_to(ROOT)}: {len(results)} cases, Spark {meta['spark_version']}")
    return 0 if all(step["ok"] for step in setup) else 1


def outcome(step: dict) -> str:
    if not step["ok"]:
        return f"error {step['condition'] or step['type']}"
    rows = step["rows"]
    if step["row_count"] <= 3:
        return "ok " + " | ".join(", ".join(row) for row in rows)[:120]
    return f"ok, {step['row_count']} rows"


def report() -> int:
    runs = {path.stem: json.loads(path.read_text()) for path in sorted(RESULTS.glob("*.json"))}
    status = 0
    if "connect-1" in runs and "connect-2" in runs:
        first, second = runs["connect-1"]["cases"], runs["connect-2"]["cases"]
        differing = [a["id"] for a, b in zip(first, second, strict=False) if a != b]
        if differing or len(first) != len(second):
            print(f"connect-1 and connect-2 differ: {', '.join(differing) or 'case count'}")
            status = 1
        else:
            print(f"connect-1 and connect-2 match in all {len(first)} cases")
    columns = [name for name in ("connect-1", "classic") if name in runs]
    if not columns:
        print("no results recorded; run with --run first")
        return 1
    indexed = {name: {case["id"]: case for case in runs[name]["cases"]} for name in columns}
    for case in runs[columns[0]]["cases"]:
        print(f"\n{case['id']} ({case['group']})")
        for index, step in enumerate(case["steps"]):
            cells = {name: outcome(indexed[name][case["id"]]["steps"][index]) for name in columns}
            differs = len(set(cells.values())) > 1
            print(f"  {index + 1}. {step['statement'].splitlines()[0][:96]}")
            for name, cell in cells.items():
                print(f"       {name:<10} {cell}{'   (differs)' if differs else ''}")
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description="Record metric view behavior.")
    parser.add_argument("--run", help="name of this run: connect-1, connect-2 or classic")
    parser.add_argument("--report", action="store_true", help="compare the recorded runs")
    parser.add_argument("--remote", default=os.environ.get("SPARK_REMOTE", "sc://localhost:15002"))
    parser.add_argument("--data", default=os.environ.get("DEMO_DATA", "/demo/data"),
                        help="the dataset directory as the Connect server sees it")
    args = parser.parse_args()
    if args.report:
        return report()
    if not args.run:
        parser.error("give --run NAME or --report")
    return record(args.run, args.remote, args.data)


if __name__ == "__main__":
    raise SystemExit(main())
