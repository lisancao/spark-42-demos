"""The demo as a storyboard: reveal the lie, then the fix — in beats, not a data dump.

    python -m metrics_views_demo.run_footgun            # run straight through
    python -m metrics_views_demo.run_footgun --pause     # wait for <enter> between beats (live demo)
    python -m metrics_views_demo.run_footgun --no-color   # plain output

Six beats, sequenced so the audience gets *fooled first* and then shown why:
  1. the number on the dashboard        (23.8% — stated with confidence)
  2. something doesn't add up           (every region is under 13% … so how?)
  3. the trap: averaging ratios         (reveal the true 9.2%)
  4. the same bug in a different hat     (MAU 69,420 vs 10,000 real users)
  5. the fix: define the metric once     (CREATE VIEW … WITH METRICS)
  6. one definition, every grain         (MEASURE() overall / region / day)

Beats 1–4 run on any Spark 4.x. Beats 5–6 need the 4.2 sandbox server and degrade
gracefully (print the error + the SQL to run by hand) if the DDL is rejected.
"""

from __future__ import annotations

import argparse
import os
import sys

from .config import SQL_DIR, get_spark, register_tables

# ----------------------------------------------------------------------------- style


class Style:
    """Minimal ANSI styling; no-ops when output isn't a terminal or color is off."""

    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def _w(self, code: str, s: str) -> str:
        return f"\033[{code}m{s}\033[0m" if self.enabled else s

    def bold(self, s: str) -> str:
        return self._w("1", s)

    def dim(self, s: str) -> str:
        return self._w("2", s)

    def red(self, s: str) -> str:
        return self._w("1;31", s)

    def green(self, s: str) -> str:
        return self._w("1;32", s)

    def yellow(self, s: str) -> str:
        return self._w("1;33", s)

    def cyan(self, s: str) -> str:
        return self._w("1;36", s)


class Narrator:
    """Prints the beats. Holds the style + the --pause behaviour."""

    def __init__(self, style: Style, pause: bool) -> None:
        self.s = style
        self._pause = pause
        self.n = 0

    def beat(self, total: int, title: str) -> None:
        self.n += 1
        bar = "─" * 66
        print(f"\n{self.s.cyan(bar)}")
        print(f"{self.s.cyan(f'  BEAT {self.n}/{total}')}   {self.s.bold(title)}")
        print(self.s.cyan(bar))

    def say(self, text: str = "") -> None:
        print(f"  {text}" if text else "")

    def big(self, label: str, value: str, tone: str = "yellow") -> None:
        color = getattr(self.s, tone)
        print(f"\n    {label}   {color(value)}\n")

    def pause(self) -> None:
        if self._pause and sys.stdin.isatty():
            try:
                input(self.s.dim("  … [enter] to continue"))
            except (EOFError, KeyboardInterrupt):
                pass


# ------------------------------------------------------------------------------ beats

TOTAL = 6


def beat1_dashboard(one_val, nar: Narrator) -> None:
    nar.beat(TOTAL, "The number on the dashboard")
    rate = one_val(
        "SELECT AVG(region_rate) FROM "
        "(SELECT region, AVG(CAST(converted AS DOUBLE)) region_rate "
        " FROM sessions GROUP BY region)"
    )
    nar.say("Monday deck. The 'National Conversion Rate' tile reads:")
    nar.big("📊  National conversion rate:", f"{rate:.1%}")
    nar.say(nar.s.dim("Built the obvious way: average each region's conversion rate."))
    nar.say(nar.s.dim("Nobody's questioned it. It's been on the slide for months."))
    nar.pause()


def beat2_doubt(spark, nar: Narrator) -> None:
    nar.beat(TOTAL, "Something doesn't add up")
    nar.say("Here's the per-region breakdown behind that tile:")
    print()
    spark.sql(
        "SELECT region, COUNT(*) AS sessions, SUM(CAST(converted AS INT)) AS converted, "
        "ROUND(AVG(CAST(converted AS DOUBLE)), 3) AS region_rate "
        "FROM sessions GROUP BY region ORDER BY sessions DESC"
    ).show(truncate=False)
    nar.say(nar.s.yellow("Every single region converts under 13%."))
    nar.say("So how does the national number come out to ~24%?")
    nar.say(nar.s.dim("A blended average can't land above every one of its parts. It can't."))
    nar.pause()


def beat3_trap(one_val, nar: Narrator) -> None:
    nar.beat(TOTAL, "The trap: averaging ratios")
    nar.say("The tile did AVG(region_rate). Five regions, equal weight — so 'remote'")
    nar.say("(200 sessions) counts exactly as much as 'metro' (50,000). The tiny,")
    nar.say("high-converting regions drag the average up.")
    naive = one_val(
        "SELECT AVG(region_rate) FROM (SELECT region, "
        "AVG(CAST(converted AS DOUBLE)) region_rate FROM sessions GROUP BY region)"
    )
    true_rate = one_val("SELECT SUM(CAST(converted AS INT)) / COUNT(*) FROM sessions")
    nar.say("\n  The honest number weights by volume — a ratio of sums, not an avg of ratios:")
    nar.big("AVG(region rate)  →", f"{naive:.1%}", "red")
    nar.big("SUM(conv)/SUM(sess) →", f"{true_rate:.1%}", "green")
    nar.say(f"The dashboard was {nar.s.red(f'{naive / true_rate:.1f}× too high')}.  "
            f"{nar.s.dim('AVG(a/b) ≠ Σa/Σb unless every b is equal.')}")
    nar.pause()


def beat4_second_hat(one_val, nar: Narrator) -> None:
    nar.beat(TOTAL, "The same bug, wearing a different hat")
    nar.say("Different tile: 'Monthly Active Users'. Built by summing daily active users.")
    naive = one_val(
        "SELECT SUM(dau) FROM (SELECT session_date, COUNT(DISTINCT user_id) dau "
        "FROM sessions GROUP BY session_date)"
    )
    nar.big("Σ daily active users →", f"{int(naive):,}")
    total_users = one_val("SELECT COUNT(*) FROM users")
    nar.say(f"Except we only have {nar.s.bold(f'{int(total_users):,}')} users in the whole system.")
    true_mau = one_val("SELECT COUNT(DISTINCT user_id) FROM sessions")
    nar.big("COUNT(DISTINCT user_id) →", f"{int(true_mau):,}", "green")
    nar.say(f"{nar.s.red(f'{naive / true_mau:.1f}× over')} — every returning user re-counted once")
    nar.say("per active day. COUNT(DISTINCT) does not distribute over SUM.")
    nar.pause()


def beat5_fix(spark, nar: Narrator) -> bool:
    nar.beat(TOTAL, "The fix: define the metric once")
    ddl = (SQL_DIR / "02_metric_view.sql").read_text()
    create_stmt = ddl[ddl.upper().index("CREATE OR REPLACE VIEW"):]
    nar.say("A Spark 4.2 metric view. The aggregation moves OUT of the query and INTO")
    nar.say("the definition — authored once, by whoever owns the metric:")
    print()
    for line in create_stmt.strip().splitlines():
        print(nar.s.dim("    │ ") + line)
    print()
    try:
        spark.sql("DROP VIEW IF EXISTS delivery_metrics")
        spark.sql(create_stmt)
    except Exception as exc:  # noqa: BLE001 — report any server rejection plainly
        nar.say(nar.s.red("⚠ The 4.2 server rejected the DDL:"))
        nar.say(f"    {type(exc).__name__}: {str(exc).splitlines()[0]}")
        nar.say(nar.s.dim("  Beats 1–4 above are real regardless. To see the fix, start the"))
        nar.say(nar.s.dim("  4.2 sandbox: docker compose -p spark42demos "
                          "-f ../../compose/docker-compose.yml up -d"))
        return False
    nar.say(nar.s.green("Created.") + "  conversion_rate IS SUM/COUNT.  active_users IS "
            "COUNT(DISTINCT).")
    nar.say(nar.s.dim("Nobody re-derives the aggregation at the call site ever again."))
    nar.pause()
    return True


def beat6_grains(spark, one_val, nar: Narrator) -> None:
    nar.beat(TOTAL, "One definition, every grain")
    nar.say("Same view, same measure names. Just change what you GROUP BY.")

    nar.say("\n  Overall — should match the honest numbers from beats 3 & 4:")
    print()
    spark.sql(
        "SELECT ROUND(MEASURE(conversion_rate), 4) AS conversion_rate, "
        "MEASURE(active_users) AS active_users, MEASURE(total_sessions) AS sessions "
        "FROM delivery_metrics"
    ).show(truncate=False)

    nar.say("By region — re-aggregated correctly, no averaging of ratios:")
    print()
    spark.sql(
        "SELECT region, MEASURE(total_sessions) AS sessions, "
        "ROUND(MEASURE(conversion_rate), 4) AS conversion_rate, "
        "MEASURE(active_users) AS active_users FROM delivery_metrics "
        "GROUP BY region ORDER BY sessions DESC"
    ).show(truncate=False)

    nar.say("By day — first days of the month:")
    print()
    spark.sql(
        "SELECT session_date, MEASURE(active_users) AS active_users, "
        "ROUND(MEASURE(conversion_rate), 4) AS conversion_rate "
        "FROM delivery_metrics GROUP BY session_date ORDER BY session_date"
    ).show(7, truncate=False)

    # The kicker: even MEASURE() won't let you add distinct counts across days.
    sum_daily = one_val(
        "SELECT SUM(active_users) FROM (SELECT session_date, MEASURE(active_users) "
        "AS active_users FROM delivery_metrics GROUP BY session_date)"
    )
    monthly = one_val("SELECT MEASURE(active_users) FROM delivery_metrics")
    nar.say("Watch the distinct count refuse to be additive:")
    nar.big("Σ daily active_users →", f"{int(sum_daily):,}", "red")
    nar.big("monthly active_users →", f"{int(monthly):,}", "green")
    nar.say(nar.s.dim("MEASURE re-counts distinct users at the day grain — it can't be summed"))
    nar.say(nar.s.dim("back up. The metric view knows the measure isn't additive, so it never"))
    nar.say(nar.s.dim("lets you add it. That's the guarantee."))
    nar.pause()


# ------------------------------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser(description="Metric Views demo — the lie, then the fix.")
    parser.add_argument("--pause", action="store_true", help="wait for <enter> between beats")
    parser.add_argument("--no-color", action="store_true", help="disable ANSI color")
    args = parser.parse_args()

    color = sys.stdout.isatty() and not args.no_color and "NO_COLOR" not in os.environ
    style = Style(color)
    nar = Narrator(style, args.pause)

    print(style.bold("\n  METRIC VIEWS — the dashboard that lied to you"))
    print(style.dim("  Spark 4.2 · food-delivery sessions (seed=42, reproducible)"))

    try:
        spark = get_spark()
    except Exception as exc:  # noqa: BLE001
        print(f"\nCould not start Spark: {exc}")
        print("Start the sandbox server, or set SPARK_LOCAL=1 for beats 1–4 only.")
        return 2

    def one_val(sql: str):
        return spark.sql(sql).collect()[0][0]

    ok = False
    try:
        register_tables(spark)
        beat1_dashboard(one_val, nar)
        beat2_doubt(spark, nar)
        beat3_trap(one_val, nar)
        beat4_second_hat(one_val, nar)
        if beat5_fix(spark, nar):
            beat6_grains(spark, one_val, nar)
            ok = True
    finally:
        spark.stop()

    print(f"\n{style.cyan('─' * 66)}")
    print(f"  {style.bold('THE POINT')}")
    print("  Two of the oldest wrong-number bugs in analytics — averaging ratios and")
    print("  summing distinct counts — stop being possible, because the aggregation")
    print("  lives with the metric instead of with whoever wrote the query.")
    if ok:
        print(style.dim("\n  Same governed number flows to SQL, BI, and an AI tool over Connect → Demo 2."))
    print()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
