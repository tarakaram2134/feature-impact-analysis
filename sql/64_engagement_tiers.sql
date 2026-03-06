-- =====================================================
-- 64_engagement_tiers.sql
-- Bucket users into baseline engagement tiers
-- =====================================================

DROP TABLE IF EXISTS marts.mart_user_engagement_tiers;

CREATE TABLE marts.mart_user_engagement_tiers AS
SELECT
  b.user_id,
  b.sessions_first_7d,

  CASE
    WHEN sessions_first_7d <= 1 THEN '0-1 sessions'
    WHEN sessions_first_7d BETWEEN 2 AND 3 THEN '2-3 sessions'
    WHEN sessions_first_7d BETWEEN 4 AND 6 THEN '4-6 sessions'
    WHEN sessions_first_7d BETWEEN 7 AND 10 THEN '7-10 sessions'
    ELSE '11+ sessions'
  END AS engagement_tier

FROM marts.mart_user_baseline_engagement b;

CREATE INDEX idx_engagement_tier
  ON marts.mart_user_engagement_tiers (engagement_tier);
