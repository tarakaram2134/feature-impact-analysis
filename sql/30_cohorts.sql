-- =====================================================
-- 30_cohorts.sql
-- User cohort table for retention and segmentation
-- =====================================================

DROP TABLE IF EXISTS marts.dim_user_cohorts;

CREATE TABLE marts.dim_user_cohorts AS
SELECT
  u.user_id,
  u.signup_at,
  DATE_TRUNC('week', u.signup_at)::date AS signup_week,
  DATE_TRUNC('month', u.signup_at)::date AS signup_month,
  u.industry,
  u.company_size,
  u.country,
  u.acquisition_channel,
  u.is_internal
FROM core.dim_users u
WHERE COALESCE(u.is_internal, false) = false;

CREATE INDEX IF NOT EXISTS idx_dim_user_cohorts_user
  ON marts.dim_user_cohorts (user_id);

CREATE INDEX IF NOT EXISTS idx_dim_user_cohorts_signup_week
  ON marts.dim_user_cohorts (signup_week);
