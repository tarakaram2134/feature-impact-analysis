-- =====================================================
-- 60_feature_impact.sql
-- Revenue impact + summary table for reporting
-- =====================================================

-- 1) User-month revenue table
DROP TABLE IF EXISTS marts.mart_user_revenue_monthly;

CREATE TABLE marts.mart_user_revenue_monthly AS
WITH inv AS (
  SELECT
    i.user_id,
    DATE_TRUNC('month', i.invoice_period_start)::date AS revenue_month,
    SUM(i.amount_usd) AS revenue_usd
  FROM core.fct_billing_invoices i
  WHERE i.status IN ('paid','open')  -- keep open to simulate delayed payment; we'll be consistent later
  GROUP BY 1,2
),

adoption AS (
  SELECT
    a.user_id,
    a.adopted_feature,
    a.feature_rollout_eligible,
    a.first_used_at
  FROM marts.fct_feature_adoption a
)

SELECT
  c.user_id,
  c.signup_month,
  inv.revenue_month,
  COALESCE(inv.revenue_usd, 0)::numeric(12,2) AS revenue_usd,

  a.adopted_feature,
  a.feature_rollout_eligible,
  a.first_used_at,

  c.industry,
  c.company_size,
  c.country,
  c.acquisition_channel

FROM marts.dim_user_cohorts c
LEFT JOIN inv
  ON c.user_id = inv.user_id
LEFT JOIN adoption a
  ON c.user_id = a.user_id;

CREATE INDEX IF NOT EXISTS idx_user_revenue_monthly
  ON marts.mart_user_revenue_monthly (revenue_month, adopted_feature);



-- 2) Feature impact summary (ARPU + lift) overall + by segments
DROP TABLE IF EXISTS marts.mart_feature_impact_summary;

CREATE TABLE marts.mart_feature_impact_summary AS
WITH base AS (
  SELECT
    revenue_month,
    adopted_feature,
    feature_rollout_eligible,
    industry,
    company_size,
    acquisition_channel,
    SUM(revenue_usd) AS total_revenue_usd,
    COUNT(DISTINCT user_id) AS users
  FROM marts.mart_user_revenue_monthly
  GROUP BY 1,2,3,4,5,6
),

arpu AS (
  SELECT
    *,
    CASE WHEN users = 0 THEN NULL
         ELSE ROUND((total_revenue_usd / users)::numeric, 2)
    END AS arpu_usd
  FROM base
)

SELECT * FROM arpu;

CREATE INDEX IF NOT EXISTS idx_feature_impact_summary_month
  ON marts.mart_feature_impact_summary (revenue_month, adopted_feature);
