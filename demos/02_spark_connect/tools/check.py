"""Check the documents against the project: links, paths, make targets, tables, names and style.

    python3 tools/check.py        # or: make check

Prints each problem as path:line and exits with status 1 if it finds any.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
THIS = Path(__file__).resolve()
DOCS = (
    "README.md",
    "blog_spark_connect.md",
    "video_spark_connect.md",
    "setup/TOPOLOGIES.md",
    "pipeline/MIGRATION.md",
    "case_study/lakehouse_stack/README.md",
    "case_study/lakehouse_stack/BUILDING_ICEBERG_FOR_SPARK_4_2.md",
)
PROJECT_DIRS = (
    "examples/", "pipeline/", "tests/", "tools/", "setup/", "case_study/", "extras/", ".vscode/",
)
ROOT_FILES = {
    "compose.yaml", "Dockerfile", "Makefile", "pyproject.toml", ".env.example", "uv.lock",
    ".python-version", "pyrightconfig.json",
}
SKIP_DIRS = {
    ".venv", ".venv-full", ".venv-41", ".git", "results", "input", "graphics", "__pycache__",
    ".pytest_cache", ".ruff_cache", ".warehouse", "target",
}
STYLE_SUFFIXES = {".py", ".md", ".sh", ".yaml", ".yml", ".json", ".toml"}

LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
TICKED = re.compile(r"`([^`\s]+)`")
TICKED_MAKE = re.compile(r"`make ([a-z][a-z0-9-]*)")
CODE_MAKE = re.compile(r"^\s*make ([a-z][a-z0-9-]*)")
CUE_MAKE = re.compile(r"\bMAKE ([A-Z][A-Z0-9-]*)")
CAPTION = re.compile(r"^\*Table (\d+(?:-\d+)?)\.")
TABLE_REF = re.compile(r"\bTable (\d+(?:-\d+)?)\b")
CUE_CONFIG = re.compile(r"LAUNCH CONFIGURATION (\d+) \(([^)]+)\)")
GRAPHIC = re.compile(r"\b(\d\d_[a-z0-9_]+\.svg)\b")
REMOVED = re.compile(
    r"(?<![\w./-])(src|migration|bench|verify)/|spark_connect_demo|\.venv-thin|\.venv-fat"
    r"|ConnectSessionFactory|which_package\(|build_remote_url|VERIFIED_FACTS"
)
# Built from parts so that this file contains none of the characters it checks for.
EM_DASH = re.compile("|".join((chr(0x2014), "\\\\" + "u2014", "&" + "mdash;")))
BRITISH = re.compile(
    r"\b(behaviour\w*|colour\w*|favour\w*|honour\w*|centre\w*|catalogue\w*|licence)\b"
    r"|\b\w*(optimis|materialis|serialis|initialis|recognis|organis|normalis|summaris|prioritis"
    r"|minimis|maximis|visualis|customis|authoris|categoris|daemonis|utilis|finalis|specialis"
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


def makefile_targets(directory: Path) -> set[str]:
    makefile = directory / "Makefile" if (directory / "Makefile").exists() else ROOT / "Makefile"
    return set(re.findall(r"^([a-z][a-z0-9-]*):", makefile.read_text(encoding="utf-8"), re.M))


def captions(path: Path) -> list[tuple[int, str]]:
    return [
        (number, match.group(1))
        for number, line, fenced in walk(path)
        if not fenced and (match := CAPTION.match(line))
    ]


def check_doc(path: Path, guide_tables: set[str]) -> None:
    targets = makefile_targets(path.parent)
    prefixes = PROJECT_DIRS
    if path.parent != ROOT:
        prefixes += tuple(f"{p.name}/" for p in path.parent.iterdir() if p.is_dir())
    tables = captions(path)
    local_tables = {table for _, table in tables}

    for number, line, fenced in walk(path):
        if removed := REMOVED.search(line):
            report(path, number, f"refers to a removed path or name: {removed[0]}")
        if fenced:
            match = CODE_MAKE.match(line)
            if match and match.group(1) not in targets:
                report(path, number, f"make target not found: {match.group(1)}")
            continue
        for target in LINK.findall(line):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            resolved = (path.parent / target.split("#")[0]).resolve()
            # Links to other material in the series lie outside the project and are not checked.
            if not resolved.is_relative_to(ROOT):
                continue
            if not resolved.exists():
                report(path, number, f"link target not found: {target}")
        for token in TICKED.findall(line):
            token = re.sub(r":\d+(-\d+)?$", "", token.rstrip(".,;"))
            if re.search(r"[*<>{}$=]", token):
                continue
            if token.startswith(prefixes) or token in ROOT_FILES:
                if not ((ROOT / token).exists() or (path.parent / token).exists()):
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

    counts: dict[str, int] = {}
    for number, table in tables:
        section, _, index = table.rpartition("-")
        counts[section] = counts.get(section, 0) + 1
        if int(index) != counts[section]:
            report(path, number, f"Table {table} is out of sequence")


def check_layers() -> None:
    """Layer names must match in the blog post, the audit legend and pipeline/MIGRATION.md."""
    guide = ROOT / "blog_spark_connect.md"
    table = guide.read_text(encoding="utf-8").split("*Table 9-1.", 1)[1].split("\n\n", 2)[1]
    expected = {int(n): name.strip().lower() for n, name in re.findall(r"^\| (\d) \| ([^|]+)\|",
                                                                      table, re.M)}
    legend = (ROOT / "tools" / "compat_audit.py").read_text(encoding="utf-8").split("LAYER", 1)[1]
    sources = {
        ROOT / "tools" / "compat_audit.py": re.findall(r"L(\d) ([a-z][a-z ]*[a-z])", legend),
        ROOT / "pipeline" / "MIGRATION.md": re.findall(
            r"^\| (\d)\. ([^|]+)\|", (ROOT / "pipeline" / "MIGRATION.md").read_text(), re.M
        ),
    }
    if len(expected) != 7:
        report(guide, 0, "Table 9-1 does not list seven layers")
    for path, pairs in sources.items():
        found = {int(n): name.strip().lower() for n, name in pairs}
        for layer, name in expected.items():
            if found.get(layer) != name:
                report(path, 0,
                       f"layer {layer} is {found.get(layer)!r}; the blog post says {name!r}")


def check_launch() -> None:
    launch = ROOT / ".vscode" / "launch.json"
    text = re.sub(r"^\s*//.*$", "", launch.read_text(encoding="utf-8"), flags=re.M)
    configurations = json.loads(text)["configurations"]
    names = {configuration["name"].lower() for configuration in configurations}
    for configuration in configurations:
        program = configuration.get("program", "").replace("${workspaceFolder}", str(ROOT))
        if program and not Path(program).exists():
            report(launch, 0, f"{configuration['name']}: program not found: {program}")
    video = ROOT / "video_spark_connect.md"
    for number, line, _ in walk(video):
        for index, name in CUE_CONFIG.findall(line):
            if f"{index}. {name}".lower() not in names:
                report(video, number, f"launch configuration not found: {index}. {name}")


def style_files():
    for directory, subdirs, files in os.walk(ROOT):
        subdirs[:] = [d for d in subdirs if d not in SKIP_DIRS]
        for name in files:
            path = Path(directory) / name
            checked = path.suffix in STYLE_SUFFIXES or name in {"Makefile", "Dockerfile"}
            if checked and path != THIS:
                yield path


def check_style() -> None:
    for path in style_files():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if EM_DASH.search(line):
                report(path, number, "em dash")
            if match := BRITISH.search(line):
                report(path, number, f"British spelling: {match[0]}")
            if path.suffix in {".py", ".md", ".yaml", ".yml"} and SPACED_DASH.search(line):
                report(path, number, "spaced -- used as a dash")


def main() -> int:
    guide_tables = {table for _, table in captions(ROOT / "blog_spark_connect.md")}
    for doc in DOCS:
        check_doc(ROOT / doc, guide_tables)
    check_layers()
    check_launch()
    check_style()
    for problem in problems:
        print(problem)
    print(f"{len(problems)} problem(s)" if problems else "no problems found")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
