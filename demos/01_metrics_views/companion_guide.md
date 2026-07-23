# Companion Guide: Metric Views in Apache Spark 4.2

*The semantic layer that stops your dashboard from lying to you.*

> Companion to Demo 1 of the Spark 4.2 series. Everything here was verified against a
> running Spark 4.2 (master/5.0-snapshot) Connect server on 2026-07-20 — the numbers,
> the YAML grammar, and the error messages are what the engine actually produced, not
> what the docs imply.

## Table of Contents

1. [The 30-second version](#1-the-30-second-version)
2. [The problem: when your dashboard lies](#2-the-problem-when-your-dashboard-lies)
   - [Footgun #1 — the average of ratios](#footgun-1--the-average-of-ratios)
   - [Footgun #2 — summing distinct counts](#footgun-2--summing-distinct-counts)
3. [What a metric view actually is](#3-what-a-metric-view-actually-is)
4. [Anatomy of the definition](#4-anatomy-of-the-definition)
5. [MEASURE(): deferred aggregation](#5-measure-deferred-aggregation)
6. [The demo, walked through](#6-the-demo-walked-through)
7. [The exact-syntax gotchas](#7-the-exact-syntax-gotchas)
8. [Metric views vs. the alternatives](#8-metric-views-vs-the-alternatives)
9. [When to reach for one (and when not to)](#9-when-to-reach-for-one-and-when-not-to)
10. [What we did not cover](#10-what-we-did-not-cover)

---

## 1. The 30-second version

A **metric view** is a new kind of Spark object (`CREATE VIEW … WITH METRICS`) where you
define your **dimensions** (things you slice by) and **measures** (numbers you aggregate)
*once*, in YAML, and — crucially — the *aggregation rule lives with the measure*. When
someone queries it, they wrap each measure in `MEASURE(...)` and the engine re-derives the
correct aggregation at whatever grain they grouped by.

Why anyone should care: the two most common ways a correct-looking SQL query returns a
*wrong* business number — averaging ratios and summing distinct counts — become
structurally impossible, because nobody re-writes the aggregation at the call site anymore.

In this demo the naive dashboard reports a **23.8% conversion rate**; the true number is
**9.2%**. The metric view returns 9.2% every time, at every grain.

---

## 2. The problem: when your dashboard lies

Both bugs below run cleanly. No error, no warning. That's what makes them dangerous — the
query is valid SQL and the number is confidently wrong.

Our dataset is a synthetic food-delivery month: `sessions` (79,000 rows), `orders`, and
`users` (10,000). Each session belongs to a `region` and may have `converted` (led to an
order). The data is deliberately engineered so both footguns fire hard.

### Footgun #1 — the average of ratios

Per-region conversion rate looks completely reasonable:

```
region     sessions  converted   rate
metro        50,000      3,937    7.9%
urban        20,000      1,979    9.9%
suburban      8,000        974   12.2%
rural           800        302   37.8%
remote          200        103   51.5%
```

Now someone builds a "national conversion rate" tile and does the obvious thing:

```sql
SELECT AVG(region_conversion_rate) FROM (
  SELECT region, AVG(CAST(converted AS DOUBLE)) AS region_conversion_rate
  FROM sessions GROUP BY region
);
-- => 0.2384   (23.8%)
```

**23.8%.** It's wrong. Averaging the five rates gives every region equal weight, so
`remote` (200 sessions, 51.5%) counts exactly as much as `metro` (50,000 sessions, 7.9%).
The tiny regions drag the average up.

The correct blended rate weights by volume — a ratio of *sums*, not an average of *ratios*:

```sql
SELECT SUM(CAST(converted AS INT)) / COUNT(*) FROM sessions;
-- => 0.0923   (9.2%)
```

The naive number is **2.6× the truth.** This is not a rounding difference; it's a different
answer to a different question, presented as if it were the one the executive asked.

> **The math:** `AVG(aᵢ/bᵢ) ≠ (Σaᵢ)/(Σbᵢ)`. They're only equal when every `bᵢ` (region
> size) is identical. They never are.

### Footgun #2 — summing distinct counts

"Monthly active users." You have a daily active-users number, so you sum it over the month:

```sql
WITH daily AS (
  SELECT session_date, COUNT(DISTINCT user_id) AS dau
  FROM sessions GROUP BY session_date
)
SELECT SUM(dau) FROM daily;
-- => 69,420
```

The true count of distinct users in the month:

```sql
SELECT COUNT(DISTINCT user_id) FROM sessions;
-- => 9,994
```

**6.9× overcounted.** Every user who came back on a second day got counted twice; a
five-day-a-week user, five times. `COUNT(DISTINCT)` does not distribute over `SUM` — you
cannot pre-aggregate it per day and add the results. But the per-day number is *right for its
grain*, so it sits in a table looking trustworthy, waiting to be summed by the next person.

The through-line of both bugs: **the correct aggregation depends on the grain of the
question, and the person writing the query has to know that.** Metric views remove that
requirement.

---

## 3. What a metric view actually is

Spark 4.2 introduces a native **semantic layer**. Concretely, a metric view is a catalog
object whose `Type` is `METRIC_VIEW` (not `VIEW`). You can see it in the catalog:

```
DESCRIBE EXTENDED delivery_metrics;

  region              string
  session_date        string
  total_sessions      bigint
  converted_sessions  bigint
  conversion_rate     double
  active_users        bigint
  ...
  Type       METRIC_VIEW
  Language   YAML
  Comment    Aggregation-safe delivery KPIs — define once, correct at every grain
```

The columns aren't rows you `SELECT *` from — a metric view holds no un-aggregated result.
`region` and `session_date` are **dimensions** you may group by; `total_sessions`,
`conversion_rate`, `active_users` are **measures** that only exist once aggregated through
`MEASURE()`. The engine understands the difference. That understanding is the whole feature.

---

## 4. Anatomy of the definition

Here is the exact, verified definition used in the demo:

```sql
CREATE OR REPLACE VIEW delivery_metrics
WITH METRICS
LANGUAGE YAML
COMMENT 'Aggregation-safe delivery KPIs — define once, correct at every grain'
AS $$
version: 0.1
source: sessions
dimensions:
  - name: region
    expr: region
  - name: session_date
    expr: session_date
measures:
  - name: total_sessions
    expr: COUNT(1)
  - name: converted_sessions
    expr: SUM(CAST(converted AS INT))
  # Ratio of aggregates — the footgun fix. NOT AVG(rate).
  - name: conversion_rate
    expr: SUM(CAST(converted AS INT)) / COUNT(1)
  # Distinct deferred to query grain — no double counting when rolled up.
  - name: active_users
    expr: COUNT(DISTINCT user_id)
$$;
```

Piece by piece:

- **`CREATE OR REPLACE VIEW … WITH METRICS LANGUAGE YAML AS $$ … $$`** — the DDL wrapper.
  The body between the `$$` markers is a YAML document, not SQL.
- **`version: 0.1`** — the metric-view spec version. This is a required, checked value.
- **`source: sessions`** — the base relation. It must be a real, persistent table (see §7).
- **`dimensions`** — each has a `name` (how you reference it) and an `expr` (how it's
  computed from the source). Here they're passthrough columns, but `expr` can be any scalar
  expression (e.g. `date_trunc('month', session_date)`).
- **`measures`** — each has a `name` and an `expr` that **contains the aggregate function**.
  This is the inversion that matters: `conversion_rate` *is* `SUM(...)/COUNT(1)`. The ratio
  is baked into the definition as a ratio-of-aggregates, so it can never be mis-aggregated
  by a caller.

The key design move: **the aggregation strategy is authored by whoever defines the metric,
not by whoever queries it.** A ratio is stored as `SUM(x)/SUM(y)`; a distinct count is stored
as `COUNT(DISTINCT ...)`. Both then evaluate correctly at any grain, because the engine
applies them *after* it knows the grouping.

---

## 5. MEASURE(): deferred aggregation

You query a metric view with ordinary `SELECT … GROUP BY`, except every measure must be
wrapped in the `MEASURE()` function:

```sql
-- Grain A: overall
SELECT MEASURE(conversion_rate) AS conversion_rate,
       MEASURE(active_users)    AS active_users,
       MEASURE(total_sessions)  AS sessions
FROM delivery_metrics;
```
```
+---------------+------------+--------+
|conversion_rate|active_users|sessions|
+---------------+------------+--------+
|0.0923         |9994        |79000   |
+---------------+------------+--------+
```

That 0.0923 **matches the hand-computed truth** from §2. Now change nothing but the grain:

```sql
-- Grain B: by region
SELECT region,
       MEASURE(total_sessions)  AS sessions,
       MEASURE(conversion_rate) AS conversion_rate,
       MEASURE(active_users)    AS active_users
FROM delivery_metrics
GROUP BY region
ORDER BY sessions DESC;
```
```
+--------+--------+---------------+------------+
|region  |sessions|conversion_rate|active_users|
+--------+--------+---------------+------------+
|metro   |50000   |0.0787         |6370        |
|urban   |20000   |0.099          |2519        |
|suburban|8000    |0.1218         |974         |
|rural   |800     |0.3775         |99          |
|remote  |200     |0.515          |32          |
+--------+--------+---------------+------------+
```

Same measure name, no re-written SQL, correct per-region rates. Group by `session_date`
instead and `active_users` will *not* sum back to 9,994 across the days — because `MEASURE`
re-evaluates `COUNT(DISTINCT user_id)` at the daily grain. That non-additivity is the point:
the metric view knows the measure isn't additive, so it never lets you add it.

**What `MEASURE()` means conceptually:** "aggregate this measure using the rule it was
defined with, at the grain implied by this query's `GROUP BY`." It is the syntactic marker
that tells Spark to look up the deferred aggregation rather than expecting a raw column.

---

## 6. The demo, walked through

The runnable project lives in `demos/01_metrics_views/`. Three commands:

```bash
# 1. Generate the engineered dataset — pure Python, no Spark needed. Instant.
python -m metrics_views_demo.generate_data
#    users: 10,000  sessions: 79,000  orders: 7,295
#    prints the punchline: naive 23.8% vs true 9.2%, 2.6x

# 2. See the lie on any local Spark 4.x (no 4.2 server required):
SPARK_LOCAL=1 python -m metrics_views_demo.run_footgun

# 3. Full lie -> fix against the 4.2 sandbox server:
docker compose -p spark42demos -f ../../compose/docker-compose.yml up -d
python -m metrics_views_demo.run_footgun
```

The generator is deterministic (`seed=42`), so the numbers in this guide reproduce exactly.
It writes Parquet directly with PyArrow — no cluster in the loop — which keeps the
data-authoring step instant and lets you iterate on the *shape* of the footgun in seconds.

`run_footgun.py` connects over Spark Connect by default (`sc://localhost:15099`) and degrades
gracefully: the "lie" half runs on any Spark, and if the metric-view DDL is rejected it prints
the error and the SQL to run by hand rather than silently skipping it.

---

## 7. The exact-syntax gotchas

These are the things the announcement blog does not tell you. Every one of them is a real
error the server threw during bring-up, now pinned down:

| Gotcha | Wrong | Right |
|--------|-------|-------|
| Spec version | `version: 1.1` → `INVALID_METRIC_VIEW_YAML` | `version: 0.1` |
| Dimensions key | `fields:` → `Unrecognized field "fields"` | `dimensions:` |
| Source object | a temp view → `INVALID_TEMP_OBJ_REFERENCE` | a **persistent** table |
| Replacing one | `CREATE OR REPLACE` over an existing metric view → `TABLE_OR_VIEW_ALREADY_EXISTS` | `DROP VIEW IF EXISTS` first |
| Ordering | `ORDER BY MEASURE(x)` where `x` isn't selected → `UNRESOLVED_COLUMN` | only order by SELECTed measures/dimensions |

Two of these bit the demo hard enough to be worth internalizing:

- **The source must be persistent.** A metric view is itself a persistent catalog object, and
  a persistent object cannot reference a temporary one. In the demo this is why
  `config.register_tables()` uses `saveAsTable(...)`, not `createOrReplaceTempView(...)`. If
  you're prototyping in a notebook with temp views, this is the first thing that will stop
  you.
- **`fields` vs `dimensions`.** Databricks' hosted YAML reference documents `fields`; the
  open-source Spark 4.2 parser wanted `dimensions`. If you're cross-reading Databricks docs
  while targeting OSS Spark, expect drift like this and trust the error message over the doc.

> The larger lesson for the reel: "the feature works, but the grammar in the blog post is not
> the grammar the engine accepts yet." Verify against a running server before you record.

---

## 8. Metric views vs. the alternatives

**vs. a plain SQL view.** A regular view can *encode* the correct aggregation, but only at one
fixed grain — the `GROUP BY` you wrote into it. Ask for a different grain and you write a new
view (or someone re-aggregates the old one and reintroduces the bug). A metric view is
grain-agnostic: one definition serves overall, per-region, per-day.

**vs. dbt metrics / MetricFlow.** dbt's semantic layer solves the same governance problem but
lives *above* the warehouse — it compiles metric requests into SQL and runs them through an
engine. Spark metric views push the semantic layer *into* the engine's catalog and optimizer,
so any Spark client (SQL, DataFrame, a BI tool over Connect, an AI tool) hits the same
governed definition without a compilation layer in between.

**vs. LookML / BI-tool semantic layers.** LookML, Power BI models, etc. are powerful but
tool-locked — the definition lives in the BI tool and only that tool honors it. A number
computed in a notebook bypasses it entirely. A Spark metric view is enforced at query time by
Spark, so the notebook and the dashboard get the same answer.

The differentiator in one line: **the semantic layer is native to the engine, so it travels
with the data instead of living in one consumer.** That's what makes the "define once,
correct in SQL *and* BI *and* an AI tool" story real rather than aspirational — and it's the
thread that connects this demo to Demo 2 (Spark Connect) and Demo 5 (vector search).

---

## 9. When to reach for one (and when not to)

**Reach for a metric view when:**
- The same KPI is consumed at multiple grains (exec rollup, per-region drill-down, daily
  trend) and must agree everywhere.
- The measure is non-additive — any ratio, rate, distinct count, or weighted average. These
  are exactly the measures that get mis-aggregated by hand.
- Multiple, heterogeneous consumers touch the number (SQL analysts, a BI tool, an AI agent)
  and you need one governed source of truth.

**Don't bother when:**
- The measure is purely additive (`SUM(revenue)`) *and* only ever consumed at one grain — a
  plain view or table is simpler.
- You need row-level output, not aggregates. A metric view has no un-aggregated form.

---

## 10. Going complex — and the grammar's real limits

We probed the running Spark 4.2 (5.0-snapshot) server to find the *actual* metric-view
grammar in this build. It is deliberately small, and it differs from the Databricks hosted
docs — so here's the verified truth, and where the real power lives.

**The whole accepted schema (probed, not guessed):**

- Top-level keys are **only**: `version`, `source`, `dimensions`, `filter`, `measures`.
  There is **no `joins`** key — the parser rejects it. Metric views in this build do not join.
- A dimension or measure has **only** `name` and `expr`. There is **no** `display_name`,
  `format`, `window`, or `synonyms` (the parser lists exactly two known properties). The
  Databricks-doc extras aren't in OSS Spark yet.
- `filter` is a global predicate applied to **every** query of the view.
- `expr` accepts arbitrarily complex SQL — **this is where all the power is.**

**So "complex" = model a wide table, then write rich `expr`s.** Since you can't join inside
the view, you denormalize first (`sessions ⟕ orders ⟕ users → session_facts`) and define the
metrics on the wide fact. That's the realistic production shape anyway — the semantic layer
sits on a modeled table. See `sql/04_complex_metric_view.sql` and
`python -m metrics_views_demo.complex_view`. It demonstrates:

- a global `filter` (`region <> 'remote'`),
- **derived dimensions** — `day_type` (`CASE … dayofweek …`), `region_tier` (`CASE … IN …`),
- **non-additive measures** that are exactly the footgun-prone kind:
  - `conversion_rate = SUM(converted)/COUNT(1)` — ratio of aggregates,
  - `aov = SUM(order_total)/NULLIF(COUNT(order_id), 0)` — guarded division,
  - `paying_users = COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN user_id END)` —
    conditional distinct,
  - `arppu = SUM(order_total)/NULLIF(COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN
    user_id END), 0)` — a ratio of a sum and a conditional distinct.

The payoff, live: overall **AOV = 32.02**, but the four `region_tier × day_type` slices read
40.90 / 34.35 / 49.69 / 40.85. Averaging the slices would be wrong; `MEASURE()` re-derives AOV
from `SUM(order_total)/COUNT(order_id)` at *each* grain — including the whole-population grain —
so it's always the honest number. Every non-additive KPI you'd normally get wrong is defined
once and safe everywhere.

**Still genuinely out of scope in this build** (rejected or absent): joins, window measures,
display/format metadata, and materialization. Those are candidates for a follow-up once the
4.2 GA grammar settles — but none of them changes the core lesson: **define the aggregation
with the metric, query it through `MEASURE()`, and the oldest wrong-number bugs in analytics
stop being possible.**

---

*Verified end-to-end against Apache Spark 4.2 (5.0.0-SNAPSHOT) Connect server, 2026-07-20.
All numbers reproduce from `seed=42`.*
