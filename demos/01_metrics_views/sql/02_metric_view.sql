-- ============================================================================
-- 02 — THE FIX.  Spark 4.2 Metric View. Define the aggregation ONCE, with the
-- measure. Requires the 4.2 sandbox server (sc://localhost:15099).
--
-- ✅ GRAMMAR VERIFIED against the running Spark 4.2 (5.0-snapshot) server, 2026-07-20:
--   * version: 0.1        (NOT 1.1 — server rejects other values: INVALID_METRIC_VIEW_YAML)
--   * key `dimensions`    (NOT `fields`)
--   * `source` must be a PERSISTENT table, not a temp view (INVALID_TEMP_OBJ_REFERENCE)
--   * every measure is queried via MEASURE(); ORDER BY may only reference SELECTed measures
--
-- The point: conversion_rate is defined as SUM(converted)/COUNT(*) (a ratio of aggregates),
-- so MEASURE() recomputes it correctly at whatever grain you group by — overall, per-region,
-- per-day — instead of averaging pre-baked ratios. active_users defers COUNT(DISTINCT) to
-- the query grain, so it never double counts on rollup.

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
