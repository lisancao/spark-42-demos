"""Tests for tools/compat_audit.py.

Each test runs the audit on a minimal snippet and checks that one rule reports it, or that no rule
does. The snippets that must not be reported matter as much as those that must.
"""

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path

import pytest
from compat_audit import SEVERITIES, audit_path, audit_source


def symbols(src: str) -> list[str]:
    return [f.symbol for f in audit_source(textwrap.dedent(src))]


def severities(src: str) -> list[str]:
    return [f.severity for f in audit_source(textwrap.dedent(src))]


# --------------------------------------------------------------------------- blockers
@pytest.mark.parametrize(
    ("src", "expected"),
    [
        ("spark.sparkContext.setLogLevel('WARN')", "setLogLevel()"),
        ("x = df.rdd.getNumPartitions()", ".rdd"),
        ("j = df._jdf", "._jdf"),
        ("v = spark._jvm.PythonUtils", "._jvm"),
        ("s2 = spark.newSession()", "newSession()"),
        ("acc = sc.accumulator(0)", "accumulator()"),
        ("bv = sc.broadcast(lookup)", "sc.broadcast()"),
        ("sc.addPyFile('helpers.py')", "addPyFile()"),
    ],
)
def test_blockers_are_detected(src: str, expected: str) -> None:
    found = symbols(src)
    assert expected in found, f"{expected!r} not reported for {src!r}; got {found}"
    assert all(s == "BLOCKER" for s in severities(src))


def test_sparkcontext_alone_is_a_blocker() -> None:
    assert ".sparkContext" in symbols("ctx = spark.sparkContext")


def test_enable_hive_support_is_a_behavior_finding() -> None:
    """Against 4.2.0 the call does not raise, so it is not a blocker (companion guide §7)."""
    findings = audit_source("b = SparkSession.builder.enableHiveSupport().getOrCreate()")
    assert [f.symbol for f in findings] == ["enableHiveSupport()"]
    assert findings[0].severity == "BEHAVIOR"
    assert "in-memory" in findings[0].message


def test_rdd_message_names_the_error_class() -> None:
    (finding,) = audit_source("x = df.rdd")
    assert "JVM_ATTRIBUTE_NOT_SUPPORTED" in finding.message


# --------------------------------------------------------------------------- receivers
def test_non_spark_setloglevel_is_not_reported() -> None:
    assert symbols("logger.setLogLevel('WARN')") == []
    assert symbols("log.setLogLevel('WARN')") == []


def test_non_spark_private_attribute_is_not_reported() -> None:
    assert symbols("x = logger._jvm") == []


def test_spark_setloglevel_is_still_reported() -> None:
    assert "setLogLevel()" in symbols("spark.sparkContext.setLogLevel('WARN')")
    assert "setLogLevel()" in symbols("sc.setLogLevel('WARN')")


def test_unknown_receiver_is_reported() -> None:
    """The audit cannot tell what an unfamiliar name refers to, so it reports it."""
    assert symbols("mystery._jvm") != []


# --------------------------------------------------------------------------- UDF decorators
def test_pandas_udf_in_a_loop_is_reported() -> None:
    src = """
        for factor in factors:
            @pandas_udf('double')
            def scale(v):
                return v * factor
    """
    found = audit_source(textwrap.dedent(src))
    assert any(f.severity == "BEHAVIOR" and "udf" in f.symbol.lower() for f in found), found


def test_aliased_udf_decorator_in_a_loop_is_reported() -> None:
    src = """
        for factor in factors:
            @spark_udf('double')
            def scale(v):
                return v * factor
    """
    found = audit_source(textwrap.dedent(src))
    assert any(f.severity == "BEHAVIOR" for f in found), found


# --------------------------------------------------------------------------- try/except
def test_action_in_the_handler_does_not_suppress_the_finding() -> None:
    """An action in the handler does not force analysis of the code in the try body."""
    src = """
        try:
            bad = df.filter(df.typo > 6)
        except AnalysisException:
            bad.collect()
            handle()
    """
    found = audit_source(textwrap.dedent(src))
    assert any("AnalysisException" in f.symbol for f in found), found


def test_specific_finding_wins_at_one_location() -> None:
    """`spark.sparkContext.setLogLevel(...)` matches two rules at one location; one is reported."""
    findings = audit_source("spark.sparkContext.setLogLevel('WARN')")
    assert len(findings) == 1
    assert findings[0].symbol == "setLogLevel()"


# --------------------------------------------------------------------------- supported code
def test_functions_broadcast_join_hint_is_not_reported() -> None:
    """The join hint is supported under Spark Connect, unlike broadcast variables."""
    for alias in ("F", "f", "functions"):
        assert symbols(f"joined = big.join({alias}.broadcast(small), 'id')") == []


def test_supported_apis_are_not_reported() -> None:
    src = """
        df = spark.read.parquet('/data/x')
        out = (df.filter(df.total > 10)
                 .groupBy('region')
                 .agg(F.count('*').alias('n')))
        out.write.mode('overwrite').saveAsTable('iceberg.gold.t')
        rows = out.collect()
        df.foreachPartition(handler)
        obs = df.observe('m', F.count(F.lit(1)).alias('rows'))
    """
    assert symbols(src) == []


def test_schema_access_outside_a_loop_is_not_reported() -> None:
    assert symbols("cols = df.columns") == []


# --------------------------------------------------------------------------- performance
def test_schema_access_in_a_loop_is_perf() -> None:
    src = """
        for name in wanted:
            if name in df.columns:
                df = df.withColumn(name, F.lit(None))
    """
    found = audit_source(textwrap.dedent(src))
    assert any(f.severity == "PERF" and "columns" in f.symbol for f in found)


def test_perf_finding_is_not_a_blocker() -> None:
    src = """
        while more:
            s = df.schema
    """
    assert "BLOCKER" not in severities(src)


# --------------------------------------------------------------------------- behavior
def test_udf_defined_in_a_loop_is_a_behavior_finding() -> None:
    src = """
        for factor in factors:
            @udf('double')
            def scale(x):
                return x * factor
            cols.append(scale('v'))
    """
    found = audit_source(textwrap.dedent(src))
    assert any(f.severity == "BEHAVIOR" and "udf" in f.symbol for f in found)


def test_udf_outside_a_loop_is_not_reported() -> None:
    src = """
        @udf('double')
        def scale(x):
            return x * 2.0
    """
    assert symbols(src) == []


def test_reused_temp_view_name_is_a_behavior_finding() -> None:
    src = """
        a.createOrReplaceTempView('brands')
        b.createOrReplaceTempView('brands')
    """
    found = audit_source(textwrap.dedent(src))
    assert any("temp view" in f.symbol and f.severity == "BEHAVIOR" for f in found)


def test_distinct_temp_view_names_are_not_reported() -> None:
    src = """
        a.createOrReplaceTempView('brands')
        b.createOrReplaceTempView('regions')
    """
    assert symbols(src) == []


def test_analysis_exception_without_an_action_is_a_behavior_finding() -> None:
    src = """
        try:
            bad = df.filter(df.typo > 6)
        except AnalysisException:
            handle()
    """
    found = audit_source(textwrap.dedent(src))
    assert any("AnalysisException" in f.symbol for f in found)


def test_analysis_exception_with_an_action_is_not_reported() -> None:
    """An action inside the try forces analysis there, so the handler can catch the error."""
    src = """
        try:
            bad = df.filter(df.typo > 6)
            bad.collect()
        except AnalysisException:
            handle()
    """
    assert symbols(src) == []


# --------------------------------------------------------------------------- output
def test_every_finding_is_well_formed() -> None:
    src = """
        spark.sparkContext.setLogLevel('WARN')
        x = df.rdd
        for c in cols:
            s = df.schema
    """
    for f in audit_source(textwrap.dedent(src), path="p.py"):
        assert f.severity in SEVERITIES
        assert f.layer.startswith("L")
        assert f.line > 0
        assert f.message and f.fix, "every finding needs a message and a fix"
        assert f.format_text()


def test_findings_are_sorted_most_severe_first() -> None:
    src = textwrap.dedent("""
        for c in cols:
            s = df.schema
        x = df.rdd
    """)
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "m.py"
        p.write_text(src)
        found = audit_path(p)
    assert [f.severity for f in found] == sorted(
        (f.severity for f in found), key=SEVERITIES.index
    )
