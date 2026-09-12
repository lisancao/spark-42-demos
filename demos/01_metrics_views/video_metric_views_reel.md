# Video: Metric Views in Apache Spark 4.2, in Five Minutes

**Alt Title:** Averaging Rates, Summing Distinct Counts, and What Metric Views Change in Spark 4.2
**Target Audience:** Spark SQL users who want the idea of metric views and one working example before watching the full video
**Estimated Runtime:** 4 to 6 minutes
**Tone:** Technical and measured. Every figure on screen comes from a measured run against Spark 4.2.0.
**Spark Version:** 4.2.0 (released 2026-07-14)

---

## VIDEO OVERVIEW

A short version of [`video_metric_views.md`](video_metric_views.md), built from its chapters 3 to 6
and reusing their footage and graphics. It shows the two aggregation errors, defines a metric view,
queries it at two grains, and ends with what a metric view does not prevent. It skips the history,
the probe and the modeled source, and points to the long-form video and the blog post for them.

**Blog post:** [Metric Views in Apache Spark 4.2: How They Work and How to Use Them](blog_metric_views.md)
**Demo project:** `spark_42_demos/demos/01_metrics_views/`. `make reel` runs the five examples the reel
uses, in order.

### Reading This Script

This script is formatted for a teleprompter, in the same way as the long-form script. Spoken text is
in the quoted paragraphs; CUE lines are presenter actions; VISUAL and ON SCREEN material is for the
editor. `video_metric_views_reel_teleprompter.txt` contains only the spoken text and cues; regenerate
it with `make teleprompter`.

---

## STORYBOARD

### CHAPTER 0. OPEN (0:00 - 0:30)

**VISUAL:** `01_title_card.svg`

**CUE:** TITLE CARD

> Spark four point two adds metric views. In the next five minutes: the two aggregation errors they're designed for, how to define one, and the one thing they don't prevent.

---

### CHAPTER 1. AVERAGING RATES (0:30 - 1:30)

**CUE:** RUN LAUNCH CONFIGURATION 3 (EXAMPLE 01, AVERAGE OF RATIOS)

**ON SCREEN:**

```
+-----------------------+-----------------------+-----+
|average_of_region_rates|converted_over_sessions|ratio|
+-----------------------+-----------------------+-----+
|                 0.2384|                 0.0923|  2.6|
+-----------------------+-----------------------+-----+
```

> Here's a month of sessions for a food delivery service, split into five regions, each with its own conversion rate.
>
> Average the five rates, and you get twenty-three point eight percent. Divide total conversions by total sessions, and you get nine point two percent.

**CUE:** SHOW AVERAGE OF RATIOS GRAPHIC

**VISUAL:** `03_average_of_ratios.svg`

> The average gives every region the same weight, so two hundred remote sessions count as much as fifty thousand metro sessions. It comes out two point six times too high.

---

### CHAPTER 2. SUMMING DISTINCT COUNTS (1:30 - 2:15)

**CUE:** RUN LAUNCH CONFIGURATION 4 (EXAMPLE 02, SUMMING DISTINCT COUNTS)

**VISUAL:** `04_summing_distinct_counts.svg`

> Second error. Add up thirty days of distinct users and you get sixty-nine thousand four hundred twenty. Count distinct users over the month and it's nine thousand nine hundred ninety-four.
>
> Each user is counted once for every day they showed up.
>
> Both queries are valid SQL. The right formula depends on the grain of the question, and nothing in the query tells Spark which one you meant.

---

### CHAPTER 3. ONE DEFINITION (2:15 - 3:15)

**CUE:** RUN LAUNCH CONFIGURATION 5 (EXAMPLE 03, CREATE A METRIC VIEW)

**VISUAL:** `05_metric_view_anatomy.svg`

> A metric view puts the formula in the catalog. You write CREATE VIEW, WITH METRICS, LANGUAGE YAML, and a YAML definition.
>
> The definition names a source table, dimensions to group by, and measures. Conversion rate is converted sessions over sessions. Active users is a distinct count.
>
> In Spark four point two point oh, the version must be zero point one, and each dimension and measure has just a name and an expression.

---

### CHAPTER 4. EVERY GRAIN (3:15 - 4:00)

**CUE:** RUN LAUNCH CONFIGURATION 6 (EXAMPLE 04, MEASURES AT EVERY GRAIN)

**VISUAL:** `06_measure_resolution.svg`

> To query it, wrap each measure in MEASURE.
>
> For the whole month, conversion rate is nine point two percent and active users is nine thousand nine hundred ninety-four. Group by region, and each region gets its own correct rate.
>
> Spark replaces MEASURE with the measure's formula and evaluates it at the query's grain. Nobody rewrites the formula.

---

### CHAPTER 5. WHAT IT DOES NOT PREVENT (4:00 - 4:40)

**CUE:** RUN LAUNCH CONFIGURATION 7 (EXAMPLE 05, WHAT IT DOES NOT PREVENT)

**ON SCREEN:**

```
+-------------------------+
|sum_of_daily_active_users|
+-------------------------+
|                    69420|
+-------------------------+
```

> Here's the limit. Ask for active users by day with MEASURE, then add those numbers up in an outer query, and you get sixty-nine thousand four hundred twenty again.
>
> The result of MEASURE is an ordinary column, and summing it is ordinary SQL. So ask for each measure at the grain you report.

---

### CHAPTER 6. CLOSE (4:40 - 5:00)

> The full video covers why metric views were proposed, the rules Spark four point two point oh enforces, and a metric view over joined tables. The blog post has all of it, with citations.

**CUE:** END CARD WITH LINKS TO THE LONG-FORM VIDEO AND THE BLOG POST

---

## PRODUCTION NOTES

### Teleprompter

Load `video_metric_views_reel_teleprompter.txt`. The pronunciation guide in the long-form script's
production notes applies here too.

**Spoken timing.** At 150 words per minute.

| Chapter | Spoken words | Spoken time |
|---|---|---|
| CHAPTER 0. OPEN (0:00 - 0:30) | 30 | 0:12 |
| CHAPTER 1. AVERAGING RATES (0:30 - 1:30) | 74 | 0:30 |
| CHAPTER 2. SUMMING DISTINCT COUNTS (1:30 - 2:15) | 67 | 0:27 |
| CHAPTER 3. ONE DEFINITION (2:15 - 3:15) | 72 | 0:29 |
| CHAPTER 4. EVERY GRAIN (3:15 - 4:00) | 57 | 0:23 |
| CHAPTER 5. WHAT IT DOES NOT PREVENT (4:00 - 4:40) | 53 | 0:21 |
| CHAPTER 6. CLOSE (4:40 - 5:00) | 34 | 0:14 |
| total spoken | 387 | 2:35 |

### Footage and Graphics

Every shot is reused from the long-form video: launch configurations 3 to 7 (chapters 3 to 6 of
`video_metric_views.md`) and graphics `01_title_card.svg`, `03_average_of_ratios.svg`,
`04_summing_distinct_counts.svg`, `05_metric_view_anatomy.svg` and `06_measure_resolution.svg`.
Nothing needs to be recorded separately.

### ON-SCREEN SAFETY CHECKLIST

The same checklist as the long-form video applies. The reel's footage shows only example output and
graphics.

### Accuracy Notes for Q&A

The long-form script's accuracy notes apply. The reel states two rules without their context: the
version must be 0.1, and each dimension and measure has only a name and an expression. Both were
measured on Spark 4.2.0 (blog post Table 4-1).
