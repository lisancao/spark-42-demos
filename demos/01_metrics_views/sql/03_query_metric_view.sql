-- ============================================================================
-- 03 — CONSISTENCY.  Query the metric view with MEASURE(). Same definition, three
-- grains, all correct — and the blended number MATCHES the naive "truth" from 01,
-- without anyone having to remember the right way to aggregate.
-- Every measure MUST be wrapped in MEASURE(); dimensions go in GROUP BY; ORDER BY
-- may only reference measures/dimensions that appear in the SELECT list.
-- ============================================================================

-- Grain A — overall. conversion_rate ~ 0.0923, active_users ~ 9994 (matches truth in 01).
SELECT
  ROUND(MEASURE(conversion_rate), 4) AS conversion_rate,
  MEASURE(active_users)              AS active_users,
  MEASURE(total_sessions)            AS sessions
FROM delivery_metrics;

-- Grain B — by region. Same measure, re-aggregated correctly per region.
SELECT
  region,
  MEASURE(total_sessions)            AS sessions,
  ROUND(MEASURE(conversion_rate), 4) AS conversion_rate,
  MEASURE(active_users)              AS active_users
FROM delivery_metrics
GROUP BY region
ORDER BY sessions DESC;

-- Grain C — by day. active_users here does NOT sum back to the monthly number —
-- because distinct is evaluated at each grain. That's the whole point.
SELECT
  session_date,
  MEASURE(active_users)              AS active_users,
  ROUND(MEASURE(conversion_rate), 4) AS conversion_rate
FROM delivery_metrics
GROUP BY session_date
ORDER BY session_date;
