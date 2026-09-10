"""Spark Connect behavior that the companion guide describes, checked against a live server."""
import os

import pytest
from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import Observation, SparkSession
from pyspark.sql import functions as F

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")


def test_server_version_is_4_2(spark):
    assert spark.version.startswith("4.2")


def test_get_or_create_returns_the_active_session_for_another_url():
    active = SparkSession.builder.remote(REMOTE).getOrCreate()
    assert SparkSession.builder.remote("sc://127.0.0.1:1").getOrCreate() is active


def test_sessions_from_create_are_isolated():
    first = SparkSession.builder.remote(REMOTE).create()
    second = SparkSession.builder.remote(REMOTE).create()
    try:
        first.range(3).createOrReplaceTempView("isolation_probe")
        second.range(7).createOrReplaceTempView("isolation_probe")
        assert first.table("isolation_probe").count() == 3
        assert second.table("isolation_probe").count() == 7
        second.stop()
        assert first.table("isolation_probe").count() == 3
    finally:
        first.stop()


def test_create_rejects_a_local_connection_string():
    with pytest.raises(PySparkException, match="UNSUPPORTED_LOCAL_CONNECTION_STRING"):
        SparkSession.builder.remote("local[2]").create()


@pytest.mark.parametrize("name", ["sparkContext", "newSession", "_jvm", "_jsc", "_jsparkSession"])
def test_session_attributes_that_need_the_jvm_raise(spark, name):
    with pytest.raises(PySparkException, match="JVM_ATTRIBUTE_NOT_SUPPORTED"):
        getattr(spark, name)


def test_dataframe_rdd_raises(spark):
    with pytest.raises(PySparkException, match="JVM_ATTRIBUTE_NOT_SUPPORTED"):
        _ = spark.range(1).rdd


def test_observe_counts_rows_during_an_action(spark):
    observation = Observation("rows")
    spark.range(25).observe(observation, F.count(F.lit(1)).alias("n")).collect()
    assert observation.get["n"] == 25


def test_broadcast_join_hint_is_supported(spark):
    left = spark.range(10).withColumnRenamed("id", "k")
    right = spark.range(5).withColumnRenamed("id", "k")
    assert left.join(F.broadcast(right), "k").count() == 5


def test_invalid_column_raises_at_the_action(spark):
    bad = spark.range(3).select("no_such_column")
    with pytest.raises(AnalysisException):
        bad.collect()


def test_schema_access_forces_analysis(spark):
    bad = spark.range(3).select("no_such_column")
    with pytest.raises(AnalysisException):
        _ = bad.schema


def test_replacing_a_temp_view_changes_an_earlier_dataframe(spark):
    spark.range(3).createOrReplaceTempView("replaced_view")
    before_replace = spark.table("replaced_view")
    spark.range(7).createOrReplaceTempView("replaced_view")
    assert before_replace.count() == 7


def test_zip_with_index(spark):
    rows = spark.createDataFrame([("a",), ("b",)], "letter string").zipWithIndex().collect()
    assert [(row["letter"], row["index"]) for row in rows] == [("a", 0), ("b", 1)]


def test_read_json_accepts_a_dataframe(spark):
    lines = spark.createDataFrame([('{"a": 1}',), ('{"a": 2}',)], "value string")
    assert spark.read.json(lines).count() == 2


def test_empty_dataframe_takes_a_schema(spark):
    assert spark.emptyDataFrame("a int").count() == 0


def test_a_slash_in_a_parameter_value_is_rejected():
    with pytest.raises(PySparkException, match="INVALID_CONNECT_URL"):
        SparkSession.builder.remote(f"{REMOTE.rstrip('/')}/;user_agent=app/1").create()


def test_a_parameter_value_without_a_slash_is_accepted():
    session = SparkSession.builder.remote(f"{REMOTE.rstrip('/')}/;user_agent=app-1").create()
    try:
        assert session.version.startswith("4.2")
    finally:
        session.stop()
