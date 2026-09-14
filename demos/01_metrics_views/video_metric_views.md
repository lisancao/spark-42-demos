# Video: Metric Views in Apache Spark 4.2: Define a Metric Once, Query It at Any Grain

**Alt Title:** Metric Views in Apache Spark 4.2: MEASURE(), the YAML Definition, and What It Does Not Prevent
**Target Audience:** Data engineers and analytics engineers who write Spark SQL and maintain metric definitions that other people query
**Estimated Runtime:** 25 to 27 minutes
**Tone:** Technical and measured; a senior engineer presenting to peers. Assumes Spark SQL fluency. Every figure on screen comes from a measured run against Spark 4.2.0.
**Spark Version:** 4.2.0 (released 2026-07-14)

---

## VIDEO OVERVIEW

The video explains why Spark added metric views, then defines and queries one against a real Spark
4.2.0 server.

It starts with two queries that return the wrong number without an error: an average of regional
conversion rates, and a sum of daily distinct users. It then defines a metric view that holds the
correct formulas, queries it at three grains with `MEASURE()`, and shows the limit of the feature:
a query can still aggregate the results of `MEASURE()` a second time. The last chapters cover the
rules Spark 4.2.0 applies to definitions, queries and catalog commands, measured with the project's
probe, and a metric view over a query that joins two tables.

Everything runs against a Spark Connect server from the official Spark 4.2.0 image, with
`pyspark-client==4.2.0` on Python 3.10. Every result on screen can be reproduced with a `make` target
or a run configuration in the companion project.

**Blog post:** [Metric Views in Apache Spark 4.2: How They Work and How to Use Them](blog_metric_views.md)
**Short version:** [`video_metric_views_reel.md`](video_metric_views_reel.md), a reel of about five
minutes built from chapters 3 to 6 of this video.
**Demo project:** `spark_42_demos/demos/01_metrics_views/`. Open it as an IDE workspace; `launch.json`
lists the run configurations in video order.

### Reading This Script

This script is formatted for a teleprompter.

- **Spoken text** is in the quoted paragraphs, one breath-length paragraph per line. Nothing else is
  read aloud. Identifiers and numbers are written the way they are said; see the pronunciation guide
  in the production notes.
- **CUE** lines are presenter actions: run a command, pause on output, switch windows.
- **VISUAL** and **ON SCREEN** material is for the editor. Tables and code under ON SCREEN appear on
  screen but are never read.
- `video_metric_views_teleprompter.txt` contains only the spoken text and cues, ready to load into a
  teleprompter app. Regenerate it after any edit with `make teleprompter`.

---

## STORYBOARD

### CHAPTER 0. COLD OPEN (0:00 - 1:30)

**VISUAL:** Terminal, full screen, the second table printed by launch configuration 3: `average_of_region_rates` 0.2384 beside `converted_over_sessions` 0.0923.

**CUE:** ROLL EXAMPLE 01 OUTPUT

> Here's one month of sessions for a food delivery service, and two ways to compute its conversion rate.
>
> One query says twenty-three point eight percent. The other says nine point two percent.
>
> Both are valid SQL, and Spark ran both without a warning.
>
> Only one of them is the conversion rate for the month.
>
> Apache Spark four point two adds metric views: a way to define a metric like this once, with its formula, and let every query evaluate that formula at the grain it asks for.
>
> In this video, we'll look at why metric views exist, how to define and query one, what they don't protect you from, and the rules Spark applies to them.
>
> Everything you'll see was run against Spark four point two point oh.

**CUE:** TITLE CARD

**VISUAL:** `01_title_card.svg`

---

### CHAPTER 1. WHY METRIC VIEWS EXIST (1:30 - 5:00)

**SECTION 1A. ADDITIVE AND NON-ADDITIVE MEASURES**

> Let's start with a distinction from dimensional modeling.
>
> Some measures are additive. Revenue for a month is the sum of the revenue for its days.
>
> Some aren't. A conversion rate is a ratio, and the rate for a month isn't the sum of the daily rates, or their average.
>
> A count of distinct users isn't additive either. Someone who shows up on five days is one user in the month, but appears in five daily counts.
>
> For ratios, the Kimball Group's advice is to keep the additive parts, and divide at the end: converted sessions over sessions, at whatever grain you need.
>
> So the correct formula is known when the metric is defined. But in plain SQL, it has to be written again every time someone queries the data.

**SECTION 1B. WHERE METRIC DEFINITIONS HAVE LIVED**

**ON SCREEN:**

> Today, users create many SQL views: one view for "active users by month," another for "active
> users by region," and so on. [...] More complex measures, such as ratios or distinct counts, often
> cannot be re-aggregated without returning incorrect results.
>
> SPIP: Metrics & semantic modeling in Spark

> In Spark, the usual answer has been views. One view for active users by month, another for active users by region.
>
> The proposal that introduced metric views describes the problem. Each view fixes its grouping when it's created, and ratios and distinct counts often can't be re-aggregated from those views without wrong results.
>
> Tools such as Looker's LookML and the dbt Semantic Layer define metrics in a modeling layer and generate SQL from those definitions. The definitions live in the tool, and a query written directly against the tables doesn't use them.

**SECTION 1C. THE PROPOSAL**

**CUE:** SHOW TIMELINE

**VISUAL:** `02_metric_views_timeline.svg`

> On October thirty-first, twenty twenty-five, Linhong Liu proposed metrics and semantic modeling for Spark on the dev mailing list.
>
> The proposal's goal is to let people define a business metric once, then ask for it by any breakdown, and always get the same answer.
>
> The vote passed on November thirteenth, with nineteen plus-one votes, five of them binding, and none against.
>
> Parsing of the YAML definitions landed in December, followed by the create command and query resolution. Support for Data Source V2 catalogs followed in May. It all shipped in Spark four point two point oh, in July twenty twenty-six.
>
> One piece is still open: composability. That's a metric view built on another metric view, or measures built from other measures. It's tracked as SPARK five four four oh eight.

**SECTION 1D. THE DESIGN**

> The design is small. A metric view is a view whose definition is a YAML document instead of a query.
>
> The document names a source, some dimensions, and some measures. A measure is an aggregate expression, written without a GROUP BY.
>
> A query asks for a measure with a function called MEASURE, and supplies the grouping itself. Spark fills in the formula at that grain.

---

### CHAPTER 2. THE DEMO SETUP (5:00 - 7:30)

**SECTION 2A. THE DATASET**

**CUE:** RUN LAUNCH CONFIGURATION 1 (GENERATE THE DATASET)

**ON SCREEN:**

```
wrote data (seed 42)
  users         10,000 rows
  sessions      79,000 rows
  orders         7,295 rows

  region      sessions  converted    rate
  metro         50,000      3,937    7.9%
  urban         20,000      1,979    9.9%
  suburban       8,000        974   12.2%
  rural            800        302   37.8%
  remote           200        103   51.5%
  unweighted average of the region rates   23.8%
  converted sessions / all sessions        9.2%
```

> The data is a synthetic month for a food delivery service: ten thousand users, seventy-nine thousand sessions, and seven thousand two hundred ninety-five orders.
>
> A script generates it with numpy and a fixed seed, so you get exactly these rows. This step doesn't need Spark.
>
> Two things about this data matter. The regions differ in size by a factor of two hundred fifty, and the small regions convert at much higher rates. And users come back on many different days.

**SECTION 2B. THE SERVER**

**CUE:** RUN MAKE UP, THEN SHOW COMPOSE.YAML

**VISUAL:** `compose.yaml`, the `spark-connect` service, with the `./data:/demo/data:ro` line highlighted.

> The server is a single Spark Connect server from the official Spark four point two point oh image, started with make up. It mounts the data directory read-only.
>
> Each example registers the Parquet files as external tables. A metric view needs a persistent source, and an external table is persistent without copying any data.
>
> The server uses Spark's in-memory catalog, so everything we create lasts until the server stops.

**CUE:** RUN LAUNCH CONFIGURATION 2 (DOCTOR)

**ON SCREEN:**

```
server
  ok   version                4.2.0
dataset and metric views
  ok   dataset                79,000 rows in /demo/data/sessions.parquet
  ok   metric view            created, queried with MEASURE() and dropped
server configuration (values of secret-like keys are masked)
       spark.sql.catalogImplementation                in-memory
all checks passed
```

> Before we start, the doctor checks the client, the server version, the mounted data, and a round trip through a metric view. All checks pass.

---

### CHAPTER 3. TWO AGGREGATION ERRORS (7:30 - 11:00)

**SECTION 3A. AVERAGING RATES**

**CUE:** RUN LAUNCH CONFIGURATION 3 (EXAMPLE 01, AVERAGE OF RATIOS)

**ON SCREEN:**

```
Conversion rate by region
+--------+--------+---------+---------------+
|  region|sessions|converted|conversion_rate|
+--------+--------+---------+---------------+
|   metro|   50000|     3937|         0.0787|
|   urban|   20000|     1979|          0.099|
|suburban|    8000|      974|         0.1218|
|   rural|     800|      302|         0.3775|
|  remote|     200|      103|          0.515|
+--------+--------+---------+---------------+

Two ways to combine the regional rates
+-----------------------+-----------------------+-----+
|average_of_region_rates|converted_over_sessions|ratio|
+-----------------------+-----------------------+-----+
|                 0.2384|                 0.0923|  2.6|
+-----------------------+-----------------------+-----+
```

> Example oh one starts with the conversion rate for each region. Metro converts seven point nine percent of its sessions. Remote converts fifty-one point five percent.
>
> Now suppose a report needs one rate for the month. The quickest query averages these five rates, and that gives twenty-three point eight percent.
>
> Divide the totals instead, seven thousand two hundred ninety-five converted sessions over seventy-nine thousand sessions, and you get nine point two percent.
>
> The average is two point six times as high.

**CUE:** SHOW AVERAGE OF RATIOS GRAPHIC

**VISUAL:** `03_average_of_ratios.svg`

> Here's why. In this chart, each region's width is its number of sessions, and its height is its conversion rate.
>
> The average gives every region the same weight. Two hundred remote sessions count as much as fifty thousand metro sessions.
>
> The rate for the month weights each region by its sessions. The two methods only agree when every region is the same size.

**SECTION 3B. SUMMING DISTINCT COUNTS**

**CUE:** RUN LAUNCH CONFIGURATION 4 (EXAMPLE 02, SUMMING DISTINCT COUNTS)

**ON SCREEN:**

```
Summing the daily counts, and counting once for the month
+-------------------------+--------------------+-----+
|sum_of_daily_active_users|monthly_active_users|users|
+-------------------------+--------------------+-----+
|                    69420|                9994|10000|
+-------------------------+--------------------+-----+
```

> Example oh two shows the second error. There's a table of daily active users, and a report needs active users for the month.
>
> Add up the thirty daily counts, and you get sixty-nine thousand four hundred twenty. But there are only ten thousand users in the users table.
>
> Count distinct users over the whole month, and you get nine thousand nine hundred ninety-four.

**CUE:** SHOW DISTINCT COUNTS GRAPHIC

**VISUAL:** `04_summing_distinct_counts.svg`

> A user with sessions on seven days shows up in seven daily counts. On average, each user here had sessions on six point nine days, so the sum is six point nine times the real count.

**SECTION 3C. WHY SPARK RUNS BOTH**

> Neither query is invalid. Nothing in them tells Spark that you wanted a rate for the month, or a count of distinct people.
>
> The right formula depends on the grain of the question, and whoever writes the query has to supply it, every time.
>
> A metric view moves that formula into a definition that every query shares.

---

### CHAPTER 4. DEFINING A METRIC VIEW (11:00 - 14:00)

**SECTION 4A. THE STATEMENT**

**CUE:** OPEN EXAMPLE 03 IN THE EDITOR

**VISUAL:** `05_metric_view_anatomy.svg`, then the editor with `examples/03_create_metric_view.py`.

**ON SCREEN:**

```yaml
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
  - name: conversion_rate
    expr: SUM(CAST(converted AS INT)) / COUNT(1)
  - name: active_users
    expr: COUNT(DISTINCT user_id)
```

> Example oh three creates the metric view. The statement is CREATE VIEW, the view's name, then WITH METRICS and LANGUAGE YAML.
>
> Both clauses are required, and YAML is the only language Spark accepts. The definition goes between two pairs of dollar signs.
>
> The definition starts with version zero point one. In Spark four point two point oh, that's the only version it accepts.
>
> The source is the sessions table. It could also be a view or a SQL query, but not a temporary view.
>
> Then two dimensions, region and session date. Those are what a query can group or filter by.
>
> And four measures. Total sessions is a count. Conversion rate is converted sessions over sessions: the formula from a minute ago. And active users is a distinct count.
>
> Each dimension and each measure has exactly two fields, a name and an expression. Anything else, such as joins, fields, or a display name, is rejected.

**SECTION 4B. WHAT SPARK RECORDS**

**CUE:** RUN LAUNCH CONFIGURATION 5 (EXAMPLE 03, CREATE A METRIC VIEW)

**ON SCREEN:**

```
Catalog entry
+----------------+-------------------------------------------------------------+-------+
|col_name        |data_type                                                    |comment|
+----------------+-------------------------------------------------------------+-------+
|Type            |METRIC_VIEW                                                  |       |
|Language        |YAML                                                         |       |
|Table Properties|[metric_view.from.name=sessions, metric_view.from.type=ASSET]|       |
+----------------+-------------------------------------------------------------+-------+
```

> Describe the view, and the dimensions and measures show up as its columns. The counts are big integers, and the rate is a double.
>
> The extended description shows the type, metric underscore view, the language, YAML, and table properties that record the source.
>
> One practical note. The example drops the view before creating it, because CREATE OR REPLACE doesn't replace an existing metric view in Spark's session catalog. It fails with table or view already exists. We'll come back to that in chapter seven.

---

### CHAPTER 5. QUERYING WITH MEASURE (14:00 - 17:30)

**SECTION 5A. ONE DEFINITION AT EVERY GRAIN**

**CUE:** RUN LAUNCH CONFIGURATION 6 (EXAMPLE 04, MEASURES AT EVERY GRAIN)

**ON SCREEN:**

```
The whole month
+---------------+------------+--------+
|conversion_rate|active_users|sessions|
+---------------+------------+--------+
|         0.0923|        9994|   79000|
+---------------+------------+--------+

By region
+--------+--------+---------------+------------+
|  region|sessions|conversion_rate|active_users|
+--------+--------+---------------+------------+
|   metro|   50000|         0.0787|        6370|
|   urban|   20000|          0.099|        2519|
|suburban|    8000|         0.1218|         974|
|   rural|     800|         0.3775|          99|
|  remote|     200|          0.515|          32|
+--------+--------+---------------+------------+
```

> Example oh four queries the view three ways.
>
> With no GROUP BY, the conversion rate measure is nine point two percent, and active users is nine thousand nine hundred ninety-four. Those are the correct numbers from chapter three.
>
> Group by region, and the same measures come back for each region, matching the regional rates from example oh one.
>
> Group by day, and active users is each day's own distinct count.
>
> Nobody rewrote a formula. The only thing that changed was the GROUP BY.

**SECTION 5B. HOW MEASURE IS RESOLVED**

**CUE:** SHOW MEASURE RESOLUTION GRAPHIC

**VISUAL:** `06_measure_resolution.svg`

> MEASURE isn't a function Spark ever executes. It marks where a measure is used.
>
> During analysis, Spark replaces MEASURE of conversion rate with the measure's expression, the sum of converted sessions over the count, and runs that aggregation over the source at the query's grain.
>
> So the aggregation that runs is the one a careful author would have written by hand. The project's tests check exactly that: at every grain, on Spark Classic and on Spark Connect, the results equal hand-written aggregates.

**SECTION 5C. QUERY CLAUSES**

**ON SCREEN:**

| Clause | Result on Spark 4.2.0 |
|---|---|
| `WHERE` on a dimension | Accepted |
| `GROUP BY ALL`, `LIMIT` | Accepted |
| `ORDER BY` or `HAVING` with the measure's alias | Accepted |
| `ORDER BY` or `HAVING` with `MEASURE()` | `UNRESOLVED_COLUMN.WITH_SUGGESTION` |
| `SELECT *`, or a measure without `MEASURE()` | `INTERNAL_ERROR` |

> A few rules for queries, measured on four point two point oh.
>
> WHERE on a dimension works. GROUP BY ALL works, and so does LIMIT.
>
> To order by a measure, use its alias or its position. ORDER BY MEASURE fails with an unresolved column error, even when the measure is selected. HAVING behaves the same way, so use the alias there too.
>
> And you can't select star from a metric view, or select a measure without MEASURE. Both fail, and in four point two point oh they fail with an internal error that doesn't say why.

---

### CHAPTER 6. WHAT A METRIC VIEW DOES NOT PREVENT (17:30 - 19:30)

**CUE:** RUN LAUNCH CONFIGURATION 7 (EXAMPLE 05, WHAT IT DOES NOT PREVENT)

**ON SCREEN:**

```
Summing daily MEASURE(active_users)
+-------------------------+
|sum_of_daily_active_users|
+-------------------------+
|                    69420|
+-------------------------+

Averaging regional MEASURE(conversion_rate)
+-----------------------+
|average_of_region_rates|
+-----------------------+
|                 0.2384|
+-----------------------+
```

> Now for the most important limit. Example oh five writes the two errors from chapter three against the metric view.
>
> The inner query asks for active users by day, and gets the right answer for each day. The outer query adds them up: sixty-nine thousand four hundred twenty, again.
>
> Average the regional conversion rates from MEASURE, and you get twenty-three point eight percent, again.
>
> To the outer query, the output of MEASURE is just a column. Summing it is ordinary SQL, and Spark runs it.
>
> And the definition can't mark a measure as non-additive. A measure has a name and an expression, and nothing else.
>
> So here's what a metric view gives you: one place for each formula, evaluated at whatever grain a query asks for.
>
> Ask for the measure at the grain you report, and you get the right number. Aggregate its results a second time, and you're back to the original error.

---

### CHAPTER 7. DEFINITION RULES AND LIFECYCLE (19:30 - 23:00)

**SECTION 7A. HOW THESE RULES WERE MEASURED**

**VISUAL:** `grammar_probe/probe.py` in the editor, then the first lines of `make probe` output: `connect-1 and connect-2 match in all 87 cases`.

> Spark four point two point oh doesn't have a documentation page for metric views. So the rules in this chapter were measured.
>
> The project's probe runs eighty-seven cases and records what Spark accepted or rejected, with the error condition.
>
> Two runs over Spark Connect gave identical results, and Spark Classic accepted and rejected the same statements.

**SECTION 7B. DEFINITIONS AND QUERIES SPARK REJECTS**

**CUE:** RUN LAUNCH CONFIGURATION 8 (EXAMPLE 06, DEFINITION RULES)

**ON SCREEN:**

```
Definitions
  accepted  the definition as written
  rejected  version: 1.0
            INVALID_METRIC_VIEW_YAML
  rejected  a joins key
            INVALID_METRIC_VIEW_YAML
  rejected  display_name on a dimension
            INVALID_METRIC_VIEW_YAML
  rejected  a temporary view as the source
            INVALID_TEMP_OBJ_REFERENCE
  rejected  a measure without an aggregate function
            MISSING_AGGREGATION
  accepted  a query as the source
```

> Example oh six tries a few definitions. Version one point oh is rejected, with invalid metric view YAML.
>
> So is a joins key, and so is a display name on a dimension. You may have seen those fields in other metric view formats, but Spark four point two point oh doesn't accept them.
>
> A temporary view as the source is rejected, and so is a measure without an aggregate function.
>
> A SQL query as the source is accepted. That includes a query with a join, which we'll use in chapter eight.

**SECTION 7C. THE LIFECYCLE OF A METRIC VIEW**

**CUE:** RUN LAUNCH CONFIGURATION 9 (EXAMPLE 07, VIEW LIFECYCLE)

**ON SCREEN:**

```
CREATE OR REPLACE VIEW lifecycle_metrics, adding active_users
  TABLE_OR_VIEW_ALREADY_EXISTS
SHOW CREATE TABLE lifecycle_metrics
  UNSUPPORTED_SHOW_CREATE_TABLE.ON_METRIC_VIEW
DESCRIBE TABLE EXTENDED lifecycle_metrics, rows Type and View Text
  Type: METRIC_VIEW
  View Text:
    version: 0.1
    source: sessions
ALTER VIEW lifecycle_metrics RENAME TO lifecycle_metrics_v2
  ok
ALTER VIEW lifecycle_metrics_v2 AS SELECT region FROM sessions
  ok
SELECT MEASURE(total_sessions) FROM lifecycle_metrics_v2
  INVALID_METRIC_VIEW_YAML
```

> Example oh seven follows a metric view through its life in the session catalog.
>
> CREATE OR REPLACE with a changed definition fails with table or view already exists, and the original definition stays. To change a metric view, drop it and create it again.
>
> SHOW CREATE TABLE isn't supported for metric views. To read a definition back, use DESCRIBE TABLE EXTENDED. Its View Text row holds the YAML.
>
> Renaming works, and the view keeps working under its new name.
>
> One more to know about: ALTER VIEW AS SELECT succeeds. It replaces the YAML with the query text, and every query after that fails with invalid metric view YAML.
>
> Dropping a metric view works with DROP VIEW, and with DROP TABLE.

---

### CHAPTER 8. MODELING THE SOURCE (23:00 - 25:30)

**CUE:** RUN LAUNCH CONFIGURATION 10 (EXAMPLE 08, MODELED SOURCE)

**ON SCREEN:**

```
The whole month, excluding the remote region
+------+---------+-----+-----+
|orders|  revenue|  aov|arppu|
+------+---------+-----+-----+
|  7192|230267.73|32.02| 46.1|
+------+---------+-----+-----+

By region tier and day type
+-----------+--------+--------+---------------+------------+-----+-----+
|region_tier|day_type|sessions|conversion_rate|paying_users|  aov|arppu|
+-----------+--------+--------+---------------+------------+-----+-----+
|       core| weekday|   51381|         0.0849|        3420|32.07| 40.9|
|       core| weekend|   18619|         0.0835|        1427|31.52|34.35|
|  long_tail| weekday|    6486|          0.138|         578|32.09|49.69|
|  long_tail| weekend|    2314|         0.1646|         310|33.24|40.85|
+-----------+--------+--------+---------------+------------+-----+-----+

Averages of the four slices
+--------------------+----------------------+
|average_of_slice_aov|average_of_slice_arppu|
+--------------------+----------------------+
|               32.23|                 41.45|
+--------------------+----------------------+
```

> Real metrics usually need more than one table. Example oh eight builds a metric view over a query that joins sessions to orders.
>
> The definition adds a filter, which applies to every query of the view. Here it leaves out the remote region.
>
> It adds two derived dimensions, weekday or weekend, and a region tier.
>
> And it adds measures for orders: revenue, average order value, and average revenue per paying user.
>
> For the month, average order value is thirty-two dollars and two cents, and revenue per paying user is forty-six dollars and ten cents.
>
> Group by tier and day type, and you get four slices. Average the slices, and you get thirty-two twenty-three and forty-one forty-five. Neither matches the month.
>
> Revenue per paying user is further off, because its denominator is a distinct count. Someone who ordered on a weekday and on a weekend is one paying user in the month, but one in each of two slices.
>
> The month's value comes only from asking for the measure at the month's grain.

---

### CHAPTER 9. SCOPE AND WHERE TO START (25:30 - 27:00)

> A few limits on what you've seen. Everything ran on Spark's session catalog, with the in-memory implementation. Data Source V2 catalogs and a Hive metastore weren't tested here, and no queries were timed.
>
> To try this yourself, open the project and run make setup, make data, make up, and then make examples.
>
> The blog post has the background, the complete tables of rules, and a citation for every claim.
>
> Metric views in Spark four point two are a small, deliberate feature: a place to put a formula, and a function that evaluates it at any grain.
>
> Use them for the metrics you report at many grains, and ask for each measure at the grain you need.
>
> Thanks for watching.

**CUE:** END CARD

---

## PRODUCTION NOTES

### Teleprompter

Load `video_metric_views_teleprompter.txt` into the teleprompter app. It contains only chapter and
section markers, bracketed cues, and the spoken text, one paragraph per breath. Regenerate it with
`make teleprompter` after editing this file; the command also prints the spoken word count per
chapter.

**Pronunciation guide.** The spoken text writes identifiers the way they are said. Use this table to
match them to what appears on screen.

| Spoken text | On screen | Say |
|---|---|---|
| MEASURE | `MEASURE()` | "measure" |
| YAML | YAML | "yam-el" |
| WITH METRICS, LANGUAGE YAML | `WITH METRICS LANGUAGE YAML` | |
| two pairs of dollar signs | `$$ ... $$` | |
| metric underscore view | `METRIC_VIEW` | |
| invalid metric view YAML | `INVALID_METRIC_VIEW_YAML` | |
| table or view already exists | `TABLE_OR_VIEW_ALREADY_EXISTS` | |
| select star | `SELECT *` | |
| GROUP BY ALL | `GROUP BY ALL` | |
| DESCRIBE TABLE EXTENDED | `DESCRIBE TABLE EXTENDED` | |
| ALTER VIEW AS SELECT | `ALTER VIEW ... AS SELECT` | |
| SPIP | SPIP | "spip" |
| SPARK five four four oh eight | SPARK-54408 | |
| Data Source V2 | Data Source V2 | "data source vee-two" |
| example oh one to example oh eight | `examples/01_average_of_ratios.py` to `examples/08_modeled_source.py` | |
| make setup, make data, make up, make examples | `make setup`, `make data`, `make up`, `make examples` | |
| average order value | `aov` | |
| average revenue per paying user | `arppu` | |
| four point two point oh | 4.2.0 | |

**Spoken timing.** At 150 words per minute. Each chapter's window also contains demo footage and
pauses on output, so spoken time should stay well inside it.

| Chapter | Spoken words | Spoken time |
|---|---|---|
| CHAPTER 0. COLD OPEN (0:00 - 1:30) | 128 | 0:51 |
| CHAPTER 1. WHY METRIC VIEWS EXIST (1:30 - 5:00) | 463 | 3:05 |
| CHAPTER 2. THE DEMO SETUP (5:00 - 7:30) | 174 | 1:10 |
| CHAPTER 3. TWO AGGREGATION ERRORS (7:30 - 11:00) | 300 | 2:00 |
| CHAPTER 4. DEFINING A METRIC VIEW (11:00 - 14:00) | 239 | 1:36 |
| CHAPTER 5. QUERYING WITH MEASURE (14:00 - 17:30) | 261 | 1:44 |
| CHAPTER 6. WHAT A METRIC VIEW DOES NOT PREVENT (17:30 - 19:30) | 154 | 1:02 |
| CHAPTER 7. DEFINITION RULES AND LIFECYCLE (19:30 - 23:00) | 267 | 1:47 |
| CHAPTER 8. MODELING THE SOURCE (23:00 - 25:30) | 172 | 1:09 |
| CHAPTER 9. SCOPE AND WHERE TO START (25:30 - 27:00) | 119 | 0:48 |
| total spoken | 2277 | 15:11 |

Chapter 1 has the least room: 3:05 of speech in a 3:30 window, carried by the timeline graphic and
the SPIP quotation rather than by demo footage.

### B-Roll / Visuals Needed

| Timestamp | Visual | Type |
|---|---|---|
| 0:00-1:30 | Launch configuration 3 output, the table of two rates, full screen; then the title card | Screen recording |
| 1:30-3:00 | The SPIP quotation, full screen | Text on screen |
| 3:00-4:30 | Timeline from proposal to release | Diagram |
| 5:00-6:00 | Launch configuration 1, the generator output | Screen recording |
| 6:00-7:30 | `make up`, `compose.yaml`, launch configuration 2 (doctor) | Screen recording |
| 7:30-9:30 | Launch configuration 3 (example 01); the average of ratios chart | Screen recording + diagram |
| 9:30-11:00 | Launch configuration 4 (example 02); the distinct counts chart | Screen recording + diagram |
| 11:00-12:30 | Metric view anatomy; `examples/03_create_metric_view.py` in the editor | Diagram + screen |
| 12:30-14:00 | Launch configuration 5 (example 03), zoom on the catalog entry | Screen recording |
| 14:00-15:30 | Launch configuration 6 (example 04), three grains | Screen recording |
| 15:30-16:30 | MEASURE resolution diagram | Diagram |
| 16:30-17:30 | Query clause table | Text on screen |
| 17:30-19:30 | Launch configuration 7 (example 05) | Screen recording |
| 19:30-20:30 | `grammar_probe/probe.py`; `make probe` comparison line | Screen recording |
| 20:30-21:30 | Launch configuration 8 (example 06) | Screen recording |
| 21:30-23:00 | Launch configuration 9 (example 07) | Screen recording |
| 23:00-25:30 | `examples/08_modeled_source.py` definition, then launch configuration 10 | Screen recording |
| 25:30-27:00 | `blog_metric_views.md` table of contents; end card | Screen recording |

### Code Demos to Pre-Record

| Demo | Chapter | Duration | Requirements |
|---|---|---|---|
| Generator output | 2 | 20 s | `make setup` |
| `make up`, then the doctor | 2 | 60 s | Docker; the dataset in `data/` |
| Example 01 | 3 | 45 s | `make up` |
| Example 02 | 3 | 45 s | `make up` |
| Example 03 | 4 | 40 s | `make up` |
| Example 04 | 5 | 60 s | Example 03 run first |
| Example 05 | 6 | 40 s | Example 03 run first |
| `make probe`, first lines of the comparison | 7 | 30 s | `make up`, `.venv-full` and a JDK |
| Example 06 | 7 | 40 s | `make up` |
| Example 07 | 7 | 45 s | `make up` |
| Example 08 | 8 | 60 s | `make up` |

### Graphics Needed

1. **`01_title_card.svg`**: title card.
2. **`02_metric_views_timeline.svg`** (Ch 1): proposal, vote, commits and release, with SPARK-54408 as still open.
3. **`03_average_of_ratios.svg`** (Ch 3): regions drawn with width proportional to sessions and height proportional to rate, with the average of the rates and the overall rate as lines.
4. **`04_summing_distinct_counts.svg`** (Ch 3): thirty daily counts stacked to 69,420 beside the month's 9,994, to scale.
5. **`05_metric_view_anatomy.svg`** (Ch 4): the statement and YAML with each part annotated.
6. **`06_measure_resolution.svg`** (Ch 5 and 6): one definition evaluated at three grains, and the note that re-aggregating the results is ordinary SQL.

Conventions: 1920×1080, dark primary with a same-named `light/` mirror, `NN_snake_case.svg`, the
series palette (background `#1a1a2e`, cards `#161b22`, borders `#30363d`, text `#e6edf3`), red for a
wrong aggregation and green for a `MEASURE()` result. The blog post's figures are separate, in
`graphics/blog/`.

### Thumbnail Concepts

- **A:** "23.8%" in red beside "9.2%" in green, with the caption "Same data, same SQL engine".
- **B:** The metric view anatomy graphic cropped to `WITH METRICS LANGUAGE YAML`, caption "Metric views in Spark 4.2".

### ON-SCREEN SAFETY CHECKLIST (review every frame before export)

The demos in this project mount no configuration files and hold no credentials.

- **Never** show `~/lakehouse-stack/config/spark/spark-defaults.conf` or any file under `~/open-lakehouse`; they contain credentials.
- **Never** show the rendered `/opt/spark/conf/spark-defaults.conf` inside a container.
- **Never** show unfiltered `docker inspect` (it prints environment variables) or `env` output.
- Record against the service in this project's `compose.yaml`, not against any lakehouse-stack container.
- `tools/doctor.py` prints three fixed configuration keys and masks the value of any key whose name contains password, secret, token, credential or access.key.
- Check the terminal prompt and window titles for a home directory path or host name before export.

### Accuracy Notes for Q&A

Likely questions, with the correct answers:

- **"Can a metric view join tables?"** The YAML format in Spark 4.2.0 has no `joins` key, and a `joins` key is rejected. The `source` can be a SQL query, and a query with a join was accepted (example 08, blog post §8).
- **"Does CREATE OR REPLACE work?"** Not in the session catalog: it fails with `TABLE_OR_VIEW_ALREADY_EXISTS`. Per the source, Data Source V2 catalogs do replace the view; that path was not tested here.
- **"Is this the same as Databricks metric views?"** The Databricks documentation describes a larger format, with specification versions 0.1 and 1.1 and fields such as `joins`, `materialization` and `display_name`. Spark 4.2.0 accepts `version`, `source`, `filter`, `dimensions` and `measures` (blog post Table 9-1).
- **"Can one metric view build on another?"** Creating one succeeded, but querying it failed with `INTERNAL_ERROR`. Composability is SPARK-54408, which is open.
- **"Does Spark stop you from re-aggregating a measure?"** No. Chapter 6 shows it, and blog post §6 explains why.
- **"Where is this documented?"** As of 2026-09-11, the Spark 4.2.0 documentation has no page on metric views; the SQL function reference has an entry for `measure`. The blog post cites the SPIP, the JIRA issues and the source.
- **"What about permissions and performance?"** Neither was tested here.

### Cross-Promotion

- The five-minute version of this video: [`video_metric_views_reel.md`](video_metric_views_reel.md).
- Demo 2 of this series covers Spark Connect, which the examples here use to reach the server
  ([blog post](../02_spark_connect/blog_spark_connect.md)).
