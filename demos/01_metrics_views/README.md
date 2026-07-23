# Demo 1 — Metric Views: *"the dashboard that lied to you"*

Spark 4.2's native semantic layer. Define a metric **once**, with its correct aggregation,
and `MEASURE()` keeps it right at every grain — killing two classics: **averaging ratios**
and **summing distinct counts**.

📖 **Deep dive:** [`companion_guide.md`](companion_guide.md) — the full written piece
(the two footguns with the math, anatomy of the definition, the verified grammar gotchas,
and metric views vs. dbt/LookML).

## The punchline (reproducible, seed=42)

| Metric | The naive SQL | Naive answer | Truth (MEASURE) | Off by |
|--------|---------------|--------------|-----------------|--------|
| Conversion rate | `AVG(per-region rate)` | **23.8%** | 9.2% | **2.6×** |
| Monthly active users | `SUM(daily distinct)` | **69,420** | 9,994 | **6.9×** |

Why: small regions (rural/remote) have high conversion and get equal weight in an average;
returning users get counted once per active day in a sum-of-distincts. The metric view defines
`conversion_rate` as `SUM(converted)/COUNT(*)` and `active_users` as `COUNT(DISTINCT user_id)`,
so the aggregation is deferred to query time and recomputed correctly per grain.

## Project layout

```
01_metrics_views/
  pyproject.toml                 # open the folder in VSCode/PyCharm; pip install -e .
  .env.example                   # -> .env (SPARK_REMOTE=sc://localhost:15099)
  src/metrics_views_demo/
    generate_data.py             # pure-python -> parquet, engineers the footgun
    config.py                    # get_spark() (Connect default / SPARK_LOCAL=1), register_tables()
    run_footgun.py               # the demo: lie -> fix, side by side
    run_footgun.py               # the demo: lie -> fix, side by side
    complex_view.py              # advanced: wide fact + rich non-additive measures
  sql/
    01_naive_wrong.sql           # the lie (runs on any Spark 4.x)
    02_metric_view.sql           # CREATE VIEW ... WITH METRICS  (needs 4.2)
    03_query_metric_view.sql     # MEASURE() at 3 grains
    04_complex_metric_view.sql   # wide fact + filter + derived dims + AOV/ARPPU (needs 4.2)
  data/                          # generated parquet (gitignored)
```

## Run it

```bash
cd demos/01_metrics_views
python -m venv .venv && source .venv/bin/activate
pip install -e .                       # resolves the `metrics_views_demo` package for the IDE

# 1) generate data (no Spark needed — instant, prints the punchline numbers)
python -m metrics_views_demo.generate_data

# 2a) see just "the lie" on your local Spark (no 4.2 server needed):
SPARK_LOCAL=1 python -m metrics_views_demo.run_footgun

# 2b) full demo incl. the metric view — start the parked 4.2 sandbox first:
cd ../..                               # spark_42_demos/
docker compose -p spark42demos -f compose/docker-compose.yml up -d
cd demos/01_metrics_views
cp .env.example .env
python -m metrics_views_demo.run_footgun

# Going deeper — a complex metric view (wide fact + filter + derived dims + AOV/ARPPU):
python -m metrics_views_demo.complex_view
```

Both halves are **verified working** (2026-07-20): the lie on local Spark 4.1, and the full
metric-view flow against the sandbox 4.2 server — overall grain returns `conversion_rate=0.0923`,
`active_users=9994`, matching the naive "truth."

## Metric-view grammar — VERIFIED on the 4.2 server

Confirmed by running against the sandbox (these bit us during bring-up, so they're locked in):

- `version: 0.1` — **not** 1.1/1.0 (server rejects others: `INVALID_METRIC_VIEW_YAML`).
- key is `dimensions` — **not** `fields`.
- `source` must be a **persistent table**, not a temp view (`INVALID_TEMP_OBJ_REFERENCE`) —
  that's why `config.register_tables()` uses `saveAsTable`, not `createOrReplaceTempView`.
- `CREATE OR REPLACE VIEW … WITH METRICS` won't replace an existing metric view — `DROP VIEW
  IF EXISTS` first.
- In queries, `ORDER BY` may only reference measures/dimensions that are in the SELECT list.
- Everything in `01_naive_wrong.sql` is version-independent and safe to show as-is.

> These are the exact-syntax gotchas the announcement blog doesn't spell out — good "watch out
> for" beats for the reel/companion guide.

## Reel script — the runner *is* the storyboard

`run_footgun.py` plays as six sequenced beats (reveal the lie *first*, then explain it). Use
`--pause` to step beat-by-beat for a live/recorded take; `--no-color` for plain capture.

```bash
python -m metrics_views_demo.run_footgun --pause
```

1. **The number on the dashboard (0:00)** — "National conversion: 23.8%." State it with
   confidence. Let it sit.
2. **Something doesn't add up (0:35)** — per-region table; *every* region is under 13%. "So how
   is the total 24%?" A blended average can't beat all its parts.
3. **The trap: averaging ratios (1:10)** — reveal `AVG(a/b) ≠ Σa/Σb`. True rate **9.2%**, the
   dashboard was 2.6× high.
4. **The same bug, different hat (1:50)** — MAU 69,420 vs only 10,000 real users → true 9,994
   (6.9× over). `COUNT(DISTINCT)` doesn't distribute over `SUM`.
5. **The fix (2:30)** — print the `CREATE VIEW … WITH METRICS` block. "The aggregation moves out
   of the query and into the definition."
6. **One definition, every grain (3:10)** — `MEASURE()` overall (matches the truth), by region,
   by day. End on the kicker: Σ daily active users **won't** sum back to monthly — the metric
   view refuses to add a distinct count.

**Close (3:50):** same governed number flows to SQL, BI, and an AI tool over Connect → teases
Demo 2.
