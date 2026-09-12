"""The Spark Connect server from compose.yaml, and the behavior recorded by grammar_probe."""
import json

import pytest

import figures
import probe

RECORDED = json.loads((probe.RESULTS / "connect-1.json").read_text(encoding="utf-8"))


def test_version(spark):
    assert spark.version == RECORDED["meta"]["spark_version"] == "4.2.0"


def test_catalog_is_in_memory(spark):
    assert spark.conf.get("spark.sql.catalogImplementation") == "in-memory"


def test_dataset_is_mounted(spark, data_dir):
    assert spark.read.parquet(f"{data_dir}/sessions.parquet").count() == figures.SESSIONS


def test_dataset_mount_is_read_only(spark, data_dir):
    with pytest.raises(Exception, match="(?i)read-only|permission|mkdirs"):
        spark.range(1).write.mode("overwrite").parquet(f"{data_dir}/write_check")


@pytest.fixture(scope="module")
def cases(spark, data_dir):
    probe.drop_probe_objects(spark, keep_base=False)
    assert all(step["ok"] for step in probe.prepare(spark, data_dir))
    yield {case["id"]: case for case in probe.build_cases(spark, data_dir)}
    probe.drop_probe_objects(spark, keep_base=False)


@pytest.mark.parametrize("recorded", RECORDED["cases"], ids=lambda case: case["id"])
def test_recorded_behavior(spark, cases, recorded):
    steps = [probe.run_step(spark, step) for step in cases[recorded["id"]]["steps"]]
    probe.drop_probe_objects(spark, keep_base=True)
    assert steps == recorded["steps"]
