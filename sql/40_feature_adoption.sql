-- =====================================================
-- 40_feature_adoption.sql
-- Feature eligibility + first exposure + first use + adoption timing
-- =====================================================

DROP TABLE IF EXISTS marts.fct_feature_adoption;

CREATE TABLE marts.fct_feature_adoption AS
WITH feature_meta AS (
  SELECT
    feature_name,
    launched_at,
    rollout_start_at,
    rollout_start_at + INTERVAL '42 days' AS rollout_end_at
  FROM core.dim_features
  WHERE feature_name = 'team_collab'
),

user_first_activity_after_launch AS (
  SELECT
    e.user_id,
    MIN(e.event_time_final) AS first_activity_after_launch
  FROM core.fct_events e
  CROSS JOIN feature_meta f
  WHERE e.event_time_final >= f.rollout_start_at
  GROUP BY 1
),

eligibility AS (
  SELECT
    u.user_id,
    u.signup_at,
    c.signup_week,
    c.signup_month,
    c.industry,
    c.company_size,
    c.country,
    c.acquisition_channel,
    a.first_activity_after_launch,
    f.rollout_start_at,
    f.rollout_end_at,

    CASE
      WHEN a.first_activity_after_launch IS NULL THEN false
      WHEN a.first_activity_after_launch >= f.rollout_end_at THEN true
      ELSE
        (
          MOD(ABS(hashtext(u.user_id)), 10000)::numeric / 10000.0
        ) <
        (
          0.20
          + 0.80 * (
              EXTRACT(EPOCH FROM (a.first_activity_after_launch - f.rollout_start_at))
              / EXTRACT(EPOCH FROM (f.rollout_end_at - f.rollout_start_at))
            )
        )
    END AS feature_rollout_eligible
  FROM core.dim_users u
  JOIN marts.dim_user_cohorts c
    ON u.user_id = c.user_id
  CROSS JOIN feature_meta f
  LEFT JOIN user_first_activity_after_launch a
    ON u.user_id = a.user_id
  WHERE COALESCE(u.is_internal, false) = false
),

first_exposure AS (
  SELECT
    user_id,
    MIN(event_time_final) AS first_exposure_at
  FROM core.fct_events
  WHERE feature_name = 'team_collab'
    AND event_name = 'feature_exposed'
  GROUP BY 1
),

first_view AS (
  SELECT
    user_id,
    MIN(event_time_final) AS first_view_at
  FROM core.fct_events
  WHERE feature_name = 'team_collab'
    AND event_name = 'feature_viewed'
  GROUP BY 1
),

first_use AS (
  SELECT
    user_id,
    MIN(event_time_final) AS first_used_at
  FROM core.fct_events
  WHERE feature_name = 'team_collab'
    AND event_name = 'feature_used'
  GROUP BY 1
),

usage_intensity AS (
  SELECT
    user_id,
    COUNT(*) FILTER (
      WHERE feature_name = 'team_collab' AND event_name = 'feature_used'
    ) AS total_feature_use_events
  FROM core.fct_events
  GROUP BY 1
)

SELECT
  e.user_id,
  e.signup_at,
  e.signup_week,
  e.signup_month,
  e.industry,
  e.company_size,
  e.country,
  e.acquisition_channel,
  e.first_activity_after_launch,
  e.feature_rollout_eligible,

  x.first_exposure_at,
  v.first_view_at,
  u.first_used_at,

  (u.first_used_at IS NOT NULL) AS adopted_feature,

  CASE
    WHEN u.first_used_at IS NOT NULL
    THEN ROUND(EXTRACT(EPOCH FROM (u.first_used_at - e.signup_at)) / 86400.0, 2)
    ELSE NULL
  END AS days_from_signup_to_adoption,

  CASE
    WHEN x.first_exposure_at IS NOT NULL AND u.first_used_at IS NOT NULL
    THEN ROUND(EXTRACT(EPOCH FROM (u.first_used_at - x.first_exposure_at)) / 86400.0, 2)
    ELSE NULL
  END AS days_from_exposure_to_adoption,

  COALESCE(i.total_feature_use_events, 0) AS total_feature_use_events

FROM eligibility e
LEFT JOIN first_exposure x
  ON e.user_id = x.user_id
LEFT JOIN first_view v
  ON e.user_id = v.user_id
LEFT JOIN first_use u
  ON e.user_id = u.user_id
LEFT JOIN usage_intensity i
  ON e.user_id = i.user_id;

CREATE INDEX IF NOT EXISTS idx_feature_adoption_user
  ON marts.fct_feature_adoption (user_id);

CREATE INDEX IF NOT EXISTS idx_feature_adoption_eligible
  ON marts.fct_feature_adoption (feature_rollout_eligible);

CREATE INDEX IF NOT EXISTS idx_feature_adoption_adopted
  ON marts.fct_feature_adoption (adopted_feature);
