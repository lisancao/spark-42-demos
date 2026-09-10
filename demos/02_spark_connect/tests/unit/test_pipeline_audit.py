"""The audit findings for the pipeline, which pipeline/MIGRATION.md quotes."""
from pathlib import Path

from compat_audit import audit_path

PIPELINE = Path(__file__).resolve().parents[2] / "pipeline"


def findings(name: str) -> list[tuple[str, str, str]]:
    return sorted((f.severity, f.layer, f.symbol) for f in audit_path(PIPELINE / name))


def test_the_classic_pipeline_has_one_finding_per_changed_layer():
    assert findings("pipeline_before.py") == sorted(
        [
            ("BLOCKER", "L1", ".rdd"),
            ("BEHAVIOR", "L3", "enableHiveSupport()"),
            ("BLOCKER", "L4", "addPyFile()"),
            ("BLOCKER", "L5", "setLogLevel()"),
        ]
    )


def test_the_migrated_pipeline_has_no_findings():
    assert findings("pipeline_after.py") == []
