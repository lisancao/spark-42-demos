-- ============================================================================
-- 01 — THE LIE.  Plain SQL that runs on ANY Spark 4.x. Both answers are WRONG.
-- Assumes temp views `sessions` and `orders` exist (run_footgun.py registers them,
-- or create them from the generated parquet — see 00_load block at the bottom).
-- ============================================================================

-- FOOTGUN #1 — ratio of ratios.
-- Per-region conversion rate looks reasonable...
SELECT region,
       COUNT(*)                        AS sessions,
       SUM(CAST(converted AS INT))     AS converted,
       AVG(CAST(converted AS DOUBLE))  AS region_conversion_rate
FROM sessions
GROUP BY region
ORDER BY sessions DESC;

-- ...but averaging those rates overstates the blended rate. Tiny high-rate regions
-- (rural/remote) get the same weight as metro. This is the number that ends up on a slide.
SELECT
  AVG(region_conversion_rate)          AS naive_avg_of_rates,   -- ~23.8%  <- THE LIE
  SUM(converted) / SUM(sessions)       AS true_blended_rate     -- ~ 9.2%  <- the truth
FROM (
  SELECT region,
         COUNT(*)                      AS sessions,
         SUM(CAST(converted AS INT))   AS converted,
         AVG(CAST(converted AS DOUBLE)) AS region_conversion_rate
  FROM sessions
  GROUP BY region
);

-- FOOTGUN #2 — distinct-count double counting.
-- Sum of daily active users counts every returning user once per active day.
WITH daily AS (
  SELECT session_date, COUNT(DISTINCT user_id) AS daily_active_users
  FROM sessions
  GROUP BY session_date
)
SELECT
  SUM(daily_active_users)                           AS naive_sum_of_daily,   -- ~69k  <- THE LIE
  (SELECT COUNT(DISTINCT user_id) FROM sessions)    AS true_monthly_active   -- ~10k  <- the truth
FROM daily;

-- ----------------------------------------------------------------------------
-- 00_load (only needed in a bare spark-sql session; replace {DATA_DIR}):
--   CREATE OR REPLACE TEMP VIEW sessions USING parquet OPTIONS (path '{DATA_DIR}/sessions.parquet');
--   CREATE OR REPLACE TEMP VIEW orders   USING parquet OPTIONS (path '{DATA_DIR}/orders.parquet');
--   CREATE OR REPLACE TEMP VIEW users    USING parquet OPTIONS (path '{DATA_DIR}/users.parquet');
-- ----------------------------------------------------------------------------
