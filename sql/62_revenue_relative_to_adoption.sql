-- =====================================================
-- 62_revenue_relative_to_adoption.sql
-- Revenue relative to adoption month (pre/post analysis)
-- =====================================================

DROP TABLE IF EXISTS marts.mart_revenue_relative_to_adoption;

CREATE TABLE marts.mart_revenue_relative_to_adoption AS
WITH inv AS (
  SELECT
    i.user_id,
    DATE_TRUNC('month', i.invoice_period_start)::date AS revenue_month,
    SUM(i.amount_usd) AS revenue_usd
  FROM core.fct_billing_invoices i
  WHERE i.status IN ('paid','open')
  GROUP BY 1,2
),

adopt AS (
  SELECT
    user_id,
    adopted_feature,
    feature_rollout_eligible,
    DATE_TRUNC('month', first_used_at)::date AS adoption_month
  FROM marts.fct_feature_adoption
),

joined AS (
  SELECT
    a.user_id,
    a.adopted_feature,
    a.feature_rollout_eligible,
    a.adoption_month,
    inv.revenue_month,
    COALESCE(inv.revenue_usd, 0)::numeric(12,2) AS revenue_usd
  FROM adopt a
  JOIN inv
    ON a.user_id = inv.user_id
  WHERE inv.revenue_month IS NOT NULL
)

SELECT
  j.*,
  CASE
    WHEN j.adoption_month IS NULL THEN NULL
    ELSE (
      (DATE_PART('year', j.revenue_month) - DATE_PART('year', j.adoption_month)) * 12
      + (DATE_PART('month', j.revenue_month) - DATE_PART('month', j.adoption_month))
    )::int
  END AS months_from_adoption
FROM joined j;

CREATE INDEX IF NOT EXISTS idx_rev_rel_adoption
  ON marts.mart_revenue_relative_to_adoption (months_from_adoption, adopted_feature);
