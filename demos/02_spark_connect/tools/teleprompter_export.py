"""Export the spoken text of a video script for a teleprompter app.

    python3 tools/teleprompter_export.py video_spark_connect.md video_spark_connect_teleprompter.txt

Reads the STORYBOARD section of the script and keeps only what the presenter needs:

- chapter headings (### CHAPTER ...)       -> === CHAPTER ... ===
- section headings (**SECTION ...**)       -> -- ... --
- cue lines (**CUE:** ...)                 -> [ ... ]
- spoken paragraphs (lines starting "> ")  -> plain paragraphs

Everything else (VISUAL notes, ON SCREEN tables and code, production notes) is omitted.
Prints the spoken word count and estimated reading time per chapter at 150 words per minute.
"""
import re
import sys

WPM = 150


def export(src: str) -> tuple[str, list[tuple[str, int]]]:
    text = open(src, encoding="utf-8").read()
    title = re.search(r"^# Video: (.+)$", text, re.M)
    body = text.split("## STORYBOARD", 1)[1].split("## PRODUCTION NOTES", 1)[0]

    out = [title.group(1).upper() if title else "VIDEO SCRIPT", ""]
    timing: list[tuple[str, int]] = []
    for line in body.splitlines():
        line = line.rstrip()
        if line.startswith("### CHAPTER"):
            name = line[4:].strip()
            timing.append((name, 0))
            out += ["", "", f"=== {name} ===", ""]
        elif m := re.match(r"^\*\*SECTION [^ .:]+[.:] (.+)\*\*$", line):
            out += ["", f"-- {m.group(1)} --", ""]
        elif m := re.match(r"^\*\*CUE:\*\* (.+)$", line):
            out += [f"[{m.group(1)}]", ""]
        elif line.startswith("> ") and line[2:].strip():
            para = line[2:].strip()
            out += [para, ""]
            if timing:
                name, words = timing[-1]
                timing[-1] = (name, words + len(para.split()))
    return re.sub(r"\n{4,}", "\n\n\n", "\n".join(out)).strip() + "\n", timing


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    rendered, timing = export(sys.argv[1])
    open(sys.argv[2], "w", encoding="utf-8").write(rendered)
    total = 0
    print(f"{'chapter':62s} {'words':>6s} {'read time':>10s}")
    for name, words in timing:
        total += words
        secs = round(words / WPM * 60)
        print(f"{name:62s} {words:6d} {secs // 60:7d}:{secs % 60:02d}")
    secs = round(total / WPM * 60)
    print(f"{'total spoken':62s} {total:6d} {secs // 60:7d}:{secs % 60:02d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
