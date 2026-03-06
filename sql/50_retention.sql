-- =====================================================
-- 50_retention.sql
-- Weekly cohort retention based on session activity
-- =====================================================

DROP TABLE IF EXISTS marts.mart_cohort_retention_weekly;

CREATE TABLE marts.mart_cohort_retention_weekly AS
WITH user_activity_weeks AS (
  SELECT
    s.user_id,
    DATE_TRUNC('week', s.session_start_at)::date AS activity_week
  FROM core.fct_sessions s
  GROUP BY 1,2
),

cohort_base AS (
  SELECT
    c.user_id,
    c.signup_week,
    a.adopted_feature
  FROM marts.dim_user_cohorts c
  JOIN marts.fct_feature_adoption a
    ON c.user_id = a.user_id
),

cohort_activity AS (
  SELECT
    cb.user_id,
    cb.signup_week,
    cb.adopted_feature,
    ua.activity_week,

    -- date - date returns integer days in Postgres
    ((ua.activity_week - cb.signup_week) / 7)::int AS week_number

  FROM cohort_base cb
  JOIN user_activity_weeks ua
    ON cb.user_id = ua.user_id
  WHERE ua.activity_week >= cb.signup_week
),

retention_counts AS (
  SELECT
    signup_week,
    adopted_feature,
    week_number,
    COUNT(DISTINCT user_id) AS active_users
  FROM cohort_activity
  GROUP BY 1,2,3
),

cohort_sizes AS (
  SELECT
    signup_week,
    adopted_feature,
    COUNT(DISTINCT user_id) AS cohort_size
  FROM cohort_base
  GROUP BY 1,2
)

SELECT
  r.signup_week,
  r.adopted_feature,
  r.week_number,
  r.active_users,
  c.cohort_size,
  ROUND(100.0 * r.active_users::numeric / c.cohort_size, 2) AS retention_rate_pct
FROM retention_counts r
JOIN cohort_sizes c
  ON r.signup_week = c.signup_week
 AND r.adopted_feature = c.adopted_feature
ORDER BY r.signup_week, r.adopted_feature, r.week_number;

CREATE INDEX IF NOT EXISTS idx_retention_cohort
  ON marts.mart_cohort_retention_weekly (signup_week, adopted_feature, week_number);
