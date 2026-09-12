"""Check the documents against the project: links, paths, names, figures, YAML and style.

    python3 tools/check.py        # or: make check

Prints each problem as path:line and exits with status 1 if it finds any.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
THIS = Path(__file__).resolve()
GUIDE = "blog_metric_views.md"
VIDEOS = ("video_metric_views.md", "video_metric_views_reel.md")
DOCS = ("README.md", GUIDE, *VIDEOS)
PROJECT_DIRS = ("examples/", "grammar_probe/", "tests/", "tools/", "graphics/", ".vscode/")
ROOT_FILES = {
    "compose.yaml", "Makefile", "pyproject.toml", ".env.example", "uv.lock", ".python-version",
    "pyrightconfig.json",
}
SKIP_DIRS = {
    ".venv", ".venv-full", ".git", "results", "graphics", "data", "__pycache__", ".pytest_cache",
    ".ruff_cache",
}
STYLE_SUFFIXES = {".py", ".md", ".sh", ".yaml", ".yml", ".json", ".toml"}
# Names that look like error conditions but are not recorded by the probe: environment variables,
# a catalog table type, and conditions the blog post cites from the Spark source.
ENVIRONMENT = {
    "SPARK_REMOTE", "DEMO_DATA", "CONNECT_PORT", "CONNECT_UI_PORT", "PYSPARK_PYTHON",
    "UV_PROJECT_ENVIRONMENT", "SPARK_HOME", "METRIC_VIEW", "WRONG_COMMAND_FOR_OBJECT_TYPE",
    "MISSING_CLAUSES_FOR_OPERATION",
}
# Numbers in the documents that are not measurements of the dataset.
OTHER_NUMBERS = {"3.10"}

LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
TICKED = re.compile(r"`([^`\s]+)`")
TICKED_MAKE = re.compile(r"`make ([a-z][a-z0-9-]*)")
CODE_MAKE = re.compile(r"^\s*make ([a-z][a-z0-9-]*)")
CUE_MAKE = re.compile(r"\bMAKE ([A-Z][A-Z0-9-]*)")
CAPTION = re.compile(r"^\*Table (\d+(?:-\d+)?)\.")
TABLE_REF = re.compile(r"\bTable (\d+(?:-\d+)?)\b")
CUE_CONFIG = re.compile(r"LAUNCH CONFIGURATION (\d+) \(([^)]+)\)")
GRAPHIC = re.compile(r"\b(\d\d_[a-z0-9_]+\.svg)\b")
CONDITION = re.compile(r"`([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+(?:\.[A-Z][A-Z0-9_]*)?)`")
# A comma-grouped integer, or a decimal with two to four places, that is not part of a version.
FIGURE = re.compile(r"(?<![\w.,])(\d{1,3}(?:,\d{3})+(?:\.\d{2})?|\d+\.\d{2,4})(?![\w.]|,\d)")
PERCENT = re.compile(r"(?<![\w.])(\d{1,3}\.\d)%")
REMOVED = re.compile(
    r"(?<![\w./-])(src|sql)/|metrics_views_demo|run_footgun|complex_view|SPARK_LOCAL"
    r"|python-dotenv|VERIFIED_FACTS|compose/docker-compose|\b15099\b|SNAPSHOT|demo_flow"
    r"|companion_guide"
)
DRAMATIC = re.compile(
    r"\b(footguns?|gotchas?|kicker|punchline|payoff|money shot|lies|lied|truth|trap)\b",
    re.IGNORECASE,
)
# Built from parts so that this file contains none of the characters it checks for.
DASH = re.compile("|".join((
    chr(0x2014), chr(0x2013), "\\\\" + "u2014", "\\\\" + "u2013", "&" + "mdash;", "&" + "ndash;",
)))
BRITISH = re.compile(
    r"\b(behaviour\w*|colour\w*|favour\w*|honour\w*|centre\w*|catalogue\w*|licence)\b"
    r"|\b\w*(optimis|materialis|serialis|initialis|recognis|organis|normalis|summaris|prioritis"
    r"|minimis|maximis|visualis|customis|authoris|categoris|utilis|finalis|specialis"
    r"|generalis|parallelis|synchronis)(e|es|ed|ing|ation|ations|er)\b"
    r"|\banalys(e|ed|ing)\b|\b(labell|travell|cancell|modell|signall)(ed|ing)\b",
    re.IGNORECASE,
)
SPACED_DASH = re.compile(r"(?:#|//|\"\"\"|^\s*[A-Za-z(`]).*\s--\s")

problems: list[str] = []


def report(path: Path, number: int, message: str) -> None:
    problems.append(f"{path.relative_to(ROOT)}:{number}: {message}")


def walk(path: Path):
    """Yield (line number, line, inside a fenced code block) for each line of a Markdown file."""
    fenced = False
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        yield number, line, fenced


def makefile_targets() -> set[str]:
    text = (ROOT / "Makefile").read_text(encoding="utf-8")
    return set(re.findall(r"^([a-z][a-z0-9-]*):", text, re.M))


def captions(path: Path) -> list[tuple[int, str]]:
    return [
        (number, match.group(1))
        for number, line, fenced in walk(path)
        if not fenced and (match := CAPTION.match(line))
    ]


def load_figures():
    spec = importlib.util.spec_from_file_location("figures", ROOT / "tests" / "figures.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def figure_forms() -> set[str]:
    """Every way the documents may write a number in tests/figures.py."""
    values: list[float] = []

    def collect(value) -> None:
        if isinstance(value, bool):
            return
        if isinstance(value, (int, float)):
            values.append(value)
        elif isinstance(value, dict):
            for item in [*value.keys(), *value.values()]:
                collect(item)
        elif isinstance(value, (tuple, list)):
            for item in value:
                collect(item)

    figures = load_figures()
    for name in dir(figures):
        if name.isupper():
            collect(getattr(figures, name))
    forms = set(OTHER_NUMBERS)
    for value in values:
        if isinstance(value, int):
            forms |= {str(value), f"{value:,}"}
        else:
            places = 4 if abs(value) < 1 else 2
            forms |= {f"{value:.{places}f}", f"{value:,.{places}f}", repr(value)}
            if abs(value) < 1:
                forms.add(f"{value * 100:.1f}")
    return forms


def recorded_conditions() -> set[str]:
    results = json.loads((ROOT / "grammar_probe" / "results" / "connect-1.json").read_text())
    return {
        step["condition"]
        for case in results["cases"]
        for step in case["steps"]
        if not step["ok"] and step["condition"]
    }


def example_definitions() -> set[str]:
    definitions = set()
    for path in sorted((ROOT / "examples").glob("*.py")):
        text = path.read_text(encoding="utf-8")
        match = re.search(r'^DEFINITION = """(.*?)"""', text, re.S | re.M)
        if match:
            definitions.add(match.group(1).strip())
    return definitions


def check_doc(path: Path, guide_tables: set[str], context: dict) -> None:
    targets = context["targets"]
    tables = captions(path)
    local_tables = {table for _, table in tables}

    for number, line, fenced in walk(path):
        if removed := REMOVED.search(line):
            report(path, number, f"refers to a removed path or name: {removed[0]}")
        for figure in FIGURE.findall(line) + PERCENT.findall(line):
            if figure not in context["figures"]:
                report(path, number, f"number not in tests/figures.py: {figure}")
        for condition in CONDITION.findall(line):
            if condition not in ENVIRONMENT and condition not in context["conditions"]:
                report(path, number, f"error condition not recorded by the probe: {condition}")
        if fenced:
            match = CODE_MAKE.match(line)
            if match and match.group(1) not in targets:
                report(path, number, f"make target not found: {match.group(1)}")
            continue
        for target in LINK.findall(line):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            resolved = (path.parent / target.split("#")[0]).resolve()
            # Links to other material in the series point outside the project and are not checked.
            if resolved.is_relative_to(ROOT) and not resolved.exists():
                report(path, number, f"link target not found: {target}")
        for token in TICKED.findall(line):
            token = re.sub(r":\d+(-\d+)?$", "", token.rstrip(".,;"))
            if re.search(r"[*<>{}$=]", token):
                continue
            if token.startswith(PROJECT_DIRS) or token in ROOT_FILES:
                if not (ROOT / token).exists():
                    report(path, number, f"path not found: {token}")
        names = TICKED_MAKE.findall(line)
        if line.startswith("**CUE:**"):
            names += [name.lower() for name in CUE_MAKE.findall(line)]
        for name in names:
            if name not in targets:
                report(path, number, f"make target not found: {name}")
        # Each video graphic has a dark version and a light version.
        for graphic in GRAPHIC.findall(line):
            for folder in (ROOT / "graphics", ROOT / "graphics" / "light"):
                expected = folder / graphic
                if not expected.exists():
                    report(path, number, f"graphic not found: {expected.relative_to(ROOT)}")
        if not CAPTION.match(line):
            for ref in TABLE_REF.findall(line):
                if ref not in local_tables and not ("blog post" in line and ref in guide_tables):
                    report(path, number, f"Table {ref} has no caption in this document")

    # Opening fences are skipped by walk(), so YAML blocks are found in a second pass.
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        if line.strip() == "```yaml":
            end = next(i for i in range(index + 1, len(lines)) if lines[i].strip() == "```")
            definition = "\n".join(lines[index + 1:end]).strip()
            if definition.startswith("version:") and definition not in context["definitions"]:
                report(path, index + 1, "YAML definition differs from every example's DEFINITION")

    counts: dict[str, int] = {}
    for number, table in tables:
        section, _, index = table.rpartition("-")
        counts[section] = counts.get(section, 0) + 1
        if int(index) != counts[section]:
            report(path, number, f"Table {table} is out of sequence")


def check_launch() -> None:
    launch = ROOT / ".vscode" / "launch.json"
    text = re.sub(r"^\s*//.*$", "", launch.read_text(encoding="utf-8"), flags=re.M)
    configurations = json.loads(text)["configurations"]
    names = {configuration["name"].lower() for configuration in configurations}
    for configuration in configurations:
        program = configuration.get("program", "").replace("${workspaceFolder}", str(ROOT))
        if program and not Path(program).exists():
            report(launch, 0, f"{configuration['name']}: program not found: {program}")
    for video in VIDEOS:
        path = ROOT / video
        if not path.exists():
            continue
        for number, line, _ in walk(path):
            for index, name in CUE_CONFIG.findall(line):
                if f"{index}. {name}".lower() not in names:
                    report(path, number, f"launch configuration not found: {index}. {name}")


def style_files():
    for directory, subdirs, files in os.walk(ROOT):
        subdirs[:] = [d for d in subdirs if d not in SKIP_DIRS]
        for name in files:
            path = Path(directory) / name
            checked = path.suffix in STYLE_SUFFIXES or name == "Makefile"
            if checked and path != THIS:
                yield path


def check_style() -> None:
    for path in style_files():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if DASH.search(line):
                report(path, number, "em or en dash")
            if match := BRITISH.search(line):
                report(path, number, f"British spelling: {match[0]}")
            if path.suffix in {".py", ".md"} and (match := DRAMATIC.search(line)):
                report(path, number, f"dramatic label: {match[0]}")
            if path.suffix in {".py", ".md", ".yaml", ".yml"} and SPACED_DASH.search(line):
                report(path, number, "spaced -- used as a dash")


def main() -> int:
    context = {
        "targets": makefile_targets(),
        "figures": figure_forms(),
        "conditions": recorded_conditions(),
        "definitions": example_definitions(),
    }
    guide = ROOT / GUIDE
    guide_tables = {table for _, table in captions(guide)} if guide.exists() else set()
    for doc in DOCS:
        if (ROOT / doc).exists():
            check_doc(ROOT / doc, guide_tables, context)
        else:
            problems.append(f"{doc}: missing")
    check_launch()
    check_style()
    for problem in problems:
        print(problem)
    print(f"{len(problems)} problem(s)" if problems else "no problems found")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
