-- =====================================================
-- 63_baseline_engagement.sql
-- Baseline engagement in first 7 days after signup
-- =====================================================

DROP TABLE IF EXISTS marts.mart_user_baseline_engagement;

CREATE TABLE marts.mart_user_baseline_engagement AS
WITH base AS (
  SELECT
    u.user_id,
    u.signup_at
  FROM core.dim_users u
),

sessions_7d AS (
  SELECT
    s.user_id,
    COUNT(*) AS sessions_first_7d
  FROM core.fct_sessions s
  JOIN base b
    ON s.user_id = b.user_id
  WHERE s.session_start_at >= b.signup_at
    AND s.session_start_at < b.signup_at + INTERVAL '7 days'
  GROUP BY 1
)

SELECT
  b.user_id,
  COALESCE(s.sessions_first_7d, 0) AS sessions_first_7d
FROM base b
LEFT JOIN sessions_7d s
  ON b.user_id = s.user_id;

CREATE INDEX idx_baseline_engagement
  ON marts.mart_user_baseline_engagement (sessions_first_7d);
