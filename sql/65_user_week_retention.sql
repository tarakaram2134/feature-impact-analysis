-- =====================================================
-- 65_user_week_retention.sql
-- User-level weekly retention flags (weeks 1–8 after signup)
-- =====================================================

DROP TABLE IF EXISTS marts.mart_user_week_retention;

CREATE TABLE marts.mart_user_week_retention AS
WITH base AS (
  SELECT
    c.user_id,
    c.signup_week,
    a.adopted_feature,
    e.engagement_tier
  FROM marts.dim_user_cohorts c
  JOIN marts.fct_feature_adoption a
    ON c.user_id = a.user_id
  JOIN marts.mart_user_engagement_tiers e
    ON c.user_id = e.user_id
),

user_weeks AS (
  -- generate weeks 0..8 for each user (so non-retained users still appear)
  SELECT
    b.user_id,
    b.signup_week,
    b.adopted_feature,
    b.engagement_tier,
    w.week_number,
    (b.signup_week + (w.week_number * INTERVAL '7 days')) AS week_start,
    (b.signup_week + ((w.week_number + 1) * INTERVAL '7 days')) AS week_end
  FROM base b
  CROSS JOIN (SELECT generate_series(0,8) AS week_number) w
),

activity AS (
  SELECT
    uw.user_id,
    uw.week_number,
    CASE WHEN COUNT(s.session_key) > 0 THEN 1 ELSE 0 END AS retained
  FROM user_weeks uw
  LEFT JOIN core.fct_sessions s
    ON s.user_id = uw.user_id
   AND s.session_start_at >= uw.week_start
   AND s.session_start_at <  uw.week_end
  GROUP BY 1,2
)

SELECT
  uw.user_id,
  uw.signup_week,
  uw.adopted_feature,
  uw.engagement_tier,
  uw.week_number,
  a.retained
FROM user_weeks uw
JOIN activity a
  ON uw.user_id = a.user_id
 AND uw.week_number = a.week_number;

CREATE INDEX idx_user_week_retention
  ON marts.mart_user_week_retention (engagement_tier, adopted_feature, week_number);
