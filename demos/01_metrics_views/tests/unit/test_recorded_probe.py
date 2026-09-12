"""The recorded probe runs come from Apache Spark 4.2.0 and agree with one another."""
import json

import pytest

import probe

RUNS = ("connect-1", "connect-2", "classic")


def load(run: str) -> dict:
    path = probe.RESULTS / f"{run}.json"
    if not path.exists():
        pytest.skip(f"{path.name} has not been recorded; run make probe")
    return json.loads(path.read_text(encoding="utf-8"))


def outcomes(result: dict) -> list:
    return [
        (case["id"], [(step["ok"], step.get("condition")) for step in case["steps"]])
        for case in result["cases"]
    ]


@pytest.mark.parametrize("run", RUNS)
def test_run_is_from_spark_4_2_0(run):
    meta = load(run)["meta"]
    assert meta["spark_version"] == "4.2.0"
    assert all(step["ok"] for step in load(run)["setup"])


def test_connect_runs_are_identical():
    assert load("connect-1")["cases"] == load("connect-2")["cases"]


def test_classic_and_connect_accept_and_reject_the_same_statements():
    assert outcomes(load("classic")) == outcomes(load("connect-1"))
