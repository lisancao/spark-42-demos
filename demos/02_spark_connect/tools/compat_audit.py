"""Report Python code that behaves differently under Spark Connect.

    python tools/compat_audit.py path/to/code [--json] [--fail-on BLOCKER] [--no-legend]

Each finding has a severity and a migration layer (blog post §9 and §12). The audit reads
source with ``ast`` and has no type information, so it matches names rather than resolving them.
"""

from __future__ import annotations

import argparse
import ast
import json
from dataclasses import asdict, dataclass
from pathlib import Path

# Py4J attributes of the Spark Classic session and DataFrame. The Connect client has no JVM.
JVM_ESCAPE_ATTRS = {
    "_jsc": "SparkSession",
    "_jconf": "SparkSession",
    "_jvm": "SparkSession",
    "_jsparkSession": "SparkSession",
    "_jdf": "DataFrame",
    "_jseq": "DataFrame",
    "_jmap": "DataFrame",
    "_jcols": "DataFrame",
}

# Receiver names that denote a SparkContext rather than pyspark.sql.functions.
SPARKCONTEXT_RECEIVERS = {"sc", "sparkContext", "spark_context", "_sc"}

# Receiver names known not to be Spark objects. Any other receiver is reported, because the audit
# cannot tell what an unfamiliar name refers to.
NON_SPARK_RECEIVERS = {
    "logger", "log", "logging", "LOG", "LOGGER", "handler", "console", "stream",
}

SCHEMA_ACCESS_ATTRS = {"columns", "schema", "dtypes"}

SEVERITIES = ("BLOCKER", "BEHAVIOR", "PERF")

EXCLUDED_DIRS = (".venv", "node_modules", "build", "__pycache__")

LEGEND = """\
Findings are tagged [SEVERITY/LAYER].

  SEVERITY   BLOCKER    raises under Spark Connect. Fix it before migrating.
             BEHAVIOR   runs, but differs from Spark Classic in its result or in when it raises.
             PERF       correct, but makes a network round trip on every iteration of a loop.

  LAYER      the part of a migration that the fix belongs to (blog post §9):
             L0 session creation        L1 pipeline logic          L2 data sources and catalogs
             L3 configuration           L4 dependencies            L5 execution and operations
             L6 security and identity
"""


@dataclass
class Finding:
    path: str
    line: int
    col: int
    severity: str
    layer: str
    symbol: str
    message: str
    fix: str

    def format_text(self, root: Path | None = None) -> str:
        p = self.path
        if root:
            try:
                p = str(Path(self.path).relative_to(root))
            except ValueError:
                pass
        return (
            f"{p}:{self.line}:{self.col} [{self.severity}/{self.layer}] {self.symbol}\n"
            f"    {self.message}\n"
            f"    fix: {self.fix}"
        )


class ConnectCompatVisitor(ast.NodeVisitor):
    # Decorator names ending in udf or udtf: udf, pandas_udf, arrow_udf, udtf, and aliases such
    # as spark_udf.
    UDF_DECORATOR_SUFFIXES = ("udf", "udtf")

    def __init__(self, path: str) -> None:
        self.path = path
        self.findings: list[Finding] = []
        self._loop_depth = 0
        self._tempview_names: dict[str, list[int]] = {}

    def _add(
        self, node: ast.AST, severity: str, layer: str, symbol: str, message: str, fix: str
    ) -> None:
        self.findings.append(
            Finding(
                path=self.path,
                line=getattr(node, "lineno", 0),
                col=getattr(node, "col_offset", 0),
                severity=severity,
                layer=layer,
                symbol=symbol,
                message=message,
                fix=fix,
            )
        )

    @staticmethod
    def _receiver_name(node: ast.AST) -> str | None:
        """Return the name that an attribute or call is reached through, if it has one."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        if isinstance(node, ast.Call):
            return ConnectCompatVisitor._receiver_name(node.func)
        return None

    def visit_Attribute(self, node: ast.Attribute) -> None:
        attr = node.attr

        if self._receiver_name(node.value) in NON_SPARK_RECEIVERS:
            self.generic_visit(node)
            return

        if attr in JVM_ESCAPE_ATTRS:
            self._add(
                node, "BLOCKER", "L1", f".{attr}",
                f"{JVM_ESCAPE_ATTRS[attr]}.{attr} is a Py4J handle to the driver JVM. The Spark "
                "Connect client has no JVM, so the attribute is not available.",
                "Express the operation with the DataFrame or SQL API, or keep this code on "
                "Spark Classic.",
            )

        elif attr == "sparkContext":
            self._add(
                node, "BLOCKER", "L1", ".sparkContext",
                "SparkContext is not available under Spark Connect (JVM_ATTRIBUTE_NOT_SUPPORTED), "
                "and neither is anything reached through it: setLogLevel(), addFile(), broadcast "
                "variables and accumulators.",
                "Replace each use: set the log level in the server's log4j2 configuration, "
                "collect metrics with DataFrame.observe(), and ship files with "
                "spark.addArtifact().",
            )

        elif attr == "rdd":
            self._add(
                node, "BLOCKER", "L1", ".rdd",
                "DataFrame.rdd raises PySparkAttributeError with error class "
                "JVM_ATTRIBUTE_NOT_SUPPORTED under Spark Connect. An RDD is an object in the "
                "driver JVM, and the protocol has no representation for it.",
                "Rewrite the logic with DataFrame operations. Spark 4.2 added "
                "DataFrame.zipWithIndex() (SPARK-55229) and DataFrame.toJSON() (SPARK-55090) to "
                "the Python client.",
            )

        elif attr in SCHEMA_ACCESS_ATTRS and self._loop_depth:
            self._add(
                node, "PERF", "L1", f".{attr} inside a loop",
                "Under Spark Connect, reading the schema of a DataFrame that has not been analyzed "
                "sends an AnalyzePlan request to the server. Inside a loop, that is a round trip "
                "per iteration.",
                "Read .columns or .schema once before the loop and reuse the result.",
            )

        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Attribute):
            name = func.attr
            recv = self._receiver_name(func.value)

            if name == "setLogLevel" and recv not in NON_SPARK_RECEIVERS:
                self._add(
                    node, "BLOCKER", "L5", "setLogLevel()",
                    "setLogLevel() is reached through SparkContext, which is not available under "
                    "Spark Connect. The log level belongs to the server's JVM, so the client has "
                    "no equivalent.",
                    "Set the log level in the Connect server's log4j2 configuration and remove "
                    "the call.",
                )

            elif name == "newSession":
                self._add(
                    node, "BLOCKER", "L1", "newSession()",
                    "SparkSession.newSession() raises JVM_ATTRIBUTE_NOT_SUPPORTED under Spark "
                    "Connect.",
                    "Create another session with SparkSession.builder.remote(url).create().",
                )

            elif name == "enableHiveSupport":
                self._add(
                    node, "BEHAVIOR", "L3", "enableHiveSupport()",
                    "enableHiveSupport() does not raise under Spark Connect. Against a 4.2.0 "
                    "server the session is created, a UserWarning reports "
                    "[CANNOT_MODIFY_STATIC_CONFIG], and spark.sql.catalogImplementation remains "
                    "'in-memory'.",
                    "Enable Hive support in the Connect server's configuration, then remove the "
                    "call.",
                )

            elif name in {"accumulator", "accumulable"}:
                self._add(
                    node, "BLOCKER", "L1", f"{name}()",
                    "Accumulators are created through SparkContext, which is not available under "
                    "Spark Connect.",
                    "Collect metrics during an action with DataFrame.observe() and an "
                    "Observation.",
                )

            elif name == "broadcast" and recv in SPARKCONTEXT_RECEIVERS:
                self._add(
                    node, "BLOCKER", "L1", "sc.broadcast()",
                    "Broadcast variables are created through SparkContext, which is not available "
                    "under Spark Connect. The join hint pyspark.sql.functions.broadcast() is a "
                    "different API and is supported.",
                    "For a join hint, use functions.broadcast(df). For lookup data, join against a "
                    "small DataFrame or pass the value as a literal column.",
                )

            elif name in {"addPyFile", "addFile"}:
                self._add(
                    node, "BLOCKER", "L4", f"{name}()",
                    f"SparkContext.{name}() is not available under Spark Connect. Files are "
                    "shipped through the session instead.",
                    "Use spark.addArtifact(path, pyfile=True) for Python modules, archive=True for "
                    "archives, or file=True for other files.",
                )

            elif name in {"createOrReplaceTempView", "createTempView"}:
                first = node.args[0] if node.args else None
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    self._tempview_names.setdefault(first.value, []).append(node.lineno)

        self.generic_visit(node)

    def _visit_loop(self, node: ast.AST) -> None:
        self._loop_depth += 1
        self.generic_visit(node)
        self._loop_depth -= 1

    visit_For = _visit_loop
    visit_While = _visit_loop
    visit_AsyncFor = _visit_loop

    def _is_udf_decorator(self, dec: ast.AST) -> bool:
        name = self._receiver_name(dec) or getattr(dec, "id", None) or ""
        return name.lower().endswith(self.UDF_DECORATOR_SUFFIXES)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        if self._loop_depth and any(self._is_udf_decorator(d) for d in node.decorator_list):
            self._add(
                node, "BEHAVIOR", "L1", f"@udf {node.name} defined in a loop",
                "Spark Connect serializes a UDF when the plan is sent, not when the UDF is "
                "defined. A UDF defined in a loop that uses the loop variable sees its final "
                "value in every iteration, where Spark Classic sees each iteration's value.",
                "Bind the value when the UDF is created, for example with a factory function, or "
                "pass it as a literal column.",
            )
        self.generic_visit(node)

    def visit_Try(self, node: ast.Try) -> None:
        catches_analysis = any(
            "AnalysisException" in ast.dump(h.type) if h.type is not None else False
            for h in node.handlers
        )
        if catches_analysis:
            # Only the try body is checked: an action in a handler does not force analysis of the
            # code that the try guards.
            has_action = any(
                isinstance(n, ast.Call)
                and isinstance(n.func, ast.Attribute)
                and n.func.attr in {"collect", "count", "show", "toPandas", "first", "take",
                                    "write", "save", "schema", "columns"}
                for stmt in node.body
                for n in ast.walk(stmt)
            )
            if not has_action:
                self._add(
                    node, "BEHAVIOR", "L1", "except AnalysisException around transformations",
                    "Spark Connect analyzes a plan when an action runs or the schema is read, so a "
                    "missing column or table raises nothing while the DataFrame is defined. This "
                    "handler does not catch that error.",
                    "Read df.schema or df.columns inside the try, or move the try around the "
                    "action.",
                )
        self.generic_visit(node)

    def _dedupe(self, findings: list[Finding]) -> list[Finding]:
        """Keep one finding per location, preferring a specific rule to the .sparkContext rule."""
        generic = {".sparkContext"}
        by_loc: dict[tuple[str, int, int], list[Finding]] = {}
        for f in findings:
            by_loc.setdefault((f.path, f.line, f.col), []).append(f)
        kept: list[Finding] = []
        for group in by_loc.values():
            specific = [f for f in group if f.symbol not in generic]
            kept.extend(specific or group)
        return kept

    def finalize(self) -> list[Finding]:
        for name, lines in self._tempview_names.items():
            if len(lines) > 1:
                self.findings.append(
                    Finding(
                        path=self.path, line=lines[1], col=0,
                        severity="BEHAVIOR", layer="L1",
                        symbol=f"temp view {name!r} registered {len(lines)} times",
                        message=(
                            "Under Spark Connect, a DataFrame read from a temp view refers to the "
                            "view by name, so registering the name again also changes DataFrames "
                            f"created from it earlier (also registered at line {lines[0]})."
                        ),
                        fix="Use a distinct name for each registration, or write the first "
                            "result out before replacing the view.",
                    )
                )
        return self._dedupe(self.findings)


def audit_source(source: str, path: str = "<string>") -> list[Finding]:
    visitor = ConnectCompatVisitor(path)
    visitor.visit(ast.parse(source))
    return visitor.finalize()


def audit_path(target: Path, excludes: tuple[str, ...] = EXCLUDED_DIRS) -> list[Finding]:
    files = [target] if target.is_file() else [
        p for p in sorted(target.rglob("*.py"))
        if not any(part in excludes for part in p.parts)
    ]
    findings: list[Finding] = []
    for f in files:
        try:
            findings.extend(audit_source(f.read_text(encoding="utf-8"), str(f)))
        except SyntaxError as exc:
            findings.append(
                Finding(str(f), exc.lineno or 0, exc.offset or 0, "BEHAVIOR", "L1",
                        "unparseable", f"The file could not be parsed: {exc.msg}",
                        "Fix the syntax error and run the audit again.")
            )
    order = {s: i for i, s in enumerate(SEVERITIES)}
    return sorted(findings, key=lambda x: (order.get(x.severity, 9), x.path, x.line))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Report Python code that behaves differently under Spark Connect.",
    )
    ap.add_argument("paths", nargs="+", type=Path)
    ap.add_argument("--json", action="store_true", help="print JSON instead of text")
    ap.add_argument("--fail-on", choices=(*SEVERITIES, "NONE"), default="BLOCKER",
                    help="exit with status 1 if a finding of this severity or a more severe one "
                         "exists; NONE always exits with status 0")
    ap.add_argument("--no-legend", action="store_true",
                    help="omit the severity and layer legend")
    args = ap.parse_args(argv)

    findings: list[Finding] = []
    for p in args.paths:
        findings.extend(audit_path(p))

    if args.json:
        print(json.dumps([asdict(f) for f in findings], indent=2))
    else:
        root = args.paths[0] if args.paths[0].is_dir() else args.paths[0].parent
        if findings and not args.no_legend:
            print(LEGEND)
        if not findings:
            print("No Spark Connect incompatibilities found.")
        for f in findings:
            print(f.format_text(root))
            print()
        counts = {s: sum(1 for f in findings if f.severity == s) for s in SEVERITIES}
        print(f"{len(findings)} finding(s): "
              + ", ".join(f"{counts[s]} {s}" for s in SEVERITIES))

    if args.fail_on != "NONE":
        threshold = SEVERITIES.index(args.fail_on)
        if any(SEVERITIES.index(f.severity) <= threshold for f in findings):
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
