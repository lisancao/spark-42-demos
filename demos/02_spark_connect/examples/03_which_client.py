"""Identify the installed PySpark distribution by its structure rather than its size.

    python examples/03_which_client.py

Companion guide §3, "Identifying the Installed Distribution".
"""
import importlib.metadata
import importlib.util
import shutil
from pathlib import Path

import pyspark


def size_mb(path: Path) -> float:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1_000_000


def version_of(distribution: str) -> str | None:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


names = ("pyspark-client", "pyspark-connect", "pyspark")
found = [f"{name} {version}" for name in names if (version := version_of(name))]
pyspark_dir = Path(pyspark.__file__).parent
jars = pyspark_dir / "jars"

print(f"distribution  {', '.join(found)}")
print(f"pyspark/      {size_mb(pyspark_dir):.0f} MB (sum of file sizes)")
if jars.is_dir():
    print(f"jars          {len(list(jars.glob('*.jar')))} files, {size_mb(jars):.0f} MB")
else:
    print("jars          none")
print(f"py4j          {'present' if importlib.util.find_spec('py4j') else 'absent'}")
print(f"java          {shutil.which('java') or 'not on PATH'}")

# pyspark-client has no jars directory and no Py4J, so it cannot start a JVM of its own.
