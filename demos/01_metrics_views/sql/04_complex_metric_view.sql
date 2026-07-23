-- ============================================================================
-- 04 — A COMPLEX metric view. Everything below is VERIFIED against the running
-- Spark 4.2 (5.0-snapshot) server, 2026-07-22.
--
-- Grammar reality of THIS build (probed, not guessed):
--   * top-level keys are ONLY: version, source, dimensions, filter, measures
--     — there is NO `joins` key. Metric views do not join. Model a wide table first.
--   * a dimension/measure has ONLY `name` + `expr` — no display_name, format, or window.
--   * `filter` is a global predicate applied to every query of the view.
--   * `expr` can be arbitrarily complex SQL — that's where the power is.
--
-- The pattern: metric views sit on top of a MODELED table. So we denormalize
-- sessions ⟕ orders ⟕ users into one wide fact, then define rich, non-additive
-- KPIs (ratios, guarded division, conditional distinct counts) on it — each correct
-- at any grain via MEASURE().
-- ============================================================================

-- 1) Model the wide fact (this is the join metric views can't do for you).
DROP TABLE IF EXISTS session_facts;
CREATE TABLE session_facts AS
SELECT  se.session_id, se.user_id, se.region, u.home_region,
        se.session_date, se.converted,
        o.order_id, o.order_total          -- NULL when the session didn't convert
FROM sessions se
LEFT JOIN orders o ON se.session_id = o.session_id
LEFT JOIN users  u ON se.user_id   = u.user_id;

-- 2) The complex metric view.
DROP VIEW IF EXISTS delivery_metrics_plus;
CREATE VIEW delivery_metrics_plus
WITH METRICS
LANGUAGE YAML
COMMENT 'Complex delivery KPIs — derived dimensions, a global filter, and non-additive measures'
AS $$
version: 0.1
source: session_facts
# Global filter — applied to EVERY query of this view. Here: drop the tiny 'remote' test region.
filter: region <> 'remote'
dimensions:
  - name: region
    expr: region
  - name: home_region
    expr: home_region
  - name: session_date
    expr: session_date
  # Derived dimension: weekend vs weekday from the date string.
  - name: day_type
    expr: CASE WHEN dayofweek(to_date(session_date)) IN (1, 7) THEN 'weekend' ELSE 'weekday' END
  # Derived dimension: bucket regions into a coarser tier.
  - name: region_tier
    expr: CASE WHEN region IN ('metro', 'urban') THEN 'core' ELSE 'long_tail' END
measures:
  - name: sessions
    expr: COUNT(1)
  # Ratio of aggregates — correct at any grain, never AVG(rate).
  - name: conversion_rate
    expr: SUM(CAST(converted AS INT)) / COUNT(1)
  # Distinct, deferred to the query grain — no double counting on rollup.
  - name: active_users
    expr: COUNT(DISTINCT user_id)
  # Conditional distinct: users who placed at least one order.
  - name: paying_users
    expr: COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN user_id END)
  - name: orders
    expr: COUNT(order_id)
  - name: revenue
    expr: SUM(order_total)
  # Average order value — guarded division (NULLIF avoids divide-by-zero at empty grains).
  - name: aov
    expr: SUM(order_total) / NULLIF(COUNT(order_id), 0)
  # Avg revenue per paying user — a ratio of a SUM and a conditional distinct count.
  - name: arppu
    expr: SUM(order_total) / NULLIF(COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN user_id END), 0)
$$;

-- 3) Query it. Every measure wrapped in MEASURE(); dimensions in GROUP BY; ORDER BY
--    may only reference SELECTed measures/dimensions.

-- Overall. AOV/ARPPU are recomputed here, NOT averaged from the slices below.
SELECT ROUND(MEASURE(aov), 2)   AS aov,       -- ~32.02
       ROUND(MEASURE(arppu), 2) AS arppu,     -- ~46.10
       MEASURE(orders)          AS orders,    -- 7192 ('remote' excluded by filter)
       ROUND(MEASURE(revenue), 2) AS revenue
FROM delivery_metrics_plus;

-- Two derived dimensions at once. Averaging any of these ratios across the four rows
-- would give a different (wrong) total — MEASURE() re-derives each at its grain.
SELECT region_tier, day_type,
       MEASURE(sessions)              AS sessions,
       ROUND(MEASURE(conversion_rate), 4) AS conversion_rate,
       MEASURE(paying_users)          AS paying_users,
       ROUND(MEASURE(aov), 2)         AS aov,
       ROUND(MEASURE(arppu), 2)       AS arppu
FROM delivery_metrics_plus
GROUP BY region_tier, day_type
ORDER BY region_tier, day_type;
