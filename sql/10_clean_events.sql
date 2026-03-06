-- =====================================================
-- 10_clean_events.sql
-- Build core.fct_events from raw.raw_events
-- Deduplicate + repair timestamps + standardize fields + DQ flags
-- =====================================================

DROP TABLE IF EXISTS core.fct_events;

CREATE TABLE core.fct_events AS
WITH
-- 1) Identify duplicates (event_ids that appear more than once in raw)
dup_event_ids AS (
  SELECT event_id
  FROM raw.raw_events
  WHERE event_id IS NOT NULL
  GROUP BY 1
  HAVING COUNT(*) > 1
),

-- 2) Deduplicate by keeping earliest ingested_at per event_id
dedup AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY r.event_id
      ORDER BY r.ingested_at ASC
    ) AS rn
  FROM raw.raw_events r
  WHERE r.event_id IS NOT NULL
),

-- 3) Keep only the deduped row
kept AS (
  SELECT
    d.event_id,
    d.user_id,
    d.event_name,
    d.event_time,
    d.feature_name,
    d.session_id,
    d.device_type,
    d.country,
    d.properties_json,
    d.ingested_at,
    (d.event_time IS NULL) AS dq_missing_event_time,
    (x.event_id IS NOT NULL) AS dq_duplicate_event_id
  FROM dedup d
  LEFT JOIN dup_event_ids x
    ON x.event_id = d.event_id
  WHERE d.rn = 1
),

-- 4) Repair event timestamp using client_event_time when event_time missing
ts_fixed AS (
  SELECT
    k.*,
    CASE
      WHEN k.event_time IS NOT NULL THEN k.event_time
      WHEN (k.properties_json ? 'client_event_time')
        THEN NULLIF((k.properties_json->>'client_event_time')::timestamptz, NULL)
      ELSE NULL
    END AS client_event_time_parsed
  FROM kept k
),

-- 5) Choose final timestamp with fallback to ingested_at
final_ts AS (
  SELECT
    t.*,
    COALESCE(t.event_time, t.client_event_time_parsed, t.ingested_at) AS event_time_final,
    (t.event_time IS NULL AND t.client_event_time_parsed IS NOT NULL) AS dq_used_client_time,
    (t.event_time IS NULL AND t.client_event_time_parsed IS NULL) AS dq_used_ingested_time
  FROM ts_fixed t
),

-- 6) Standardize device/country
standardized AS (
  SELECT
    f.*,
    CASE
      WHEN f.device_type IN ('desktop','mobile','tablet') THEN f.device_type
      ELSE 'unknown'
    END AS device_type_std,
    CASE
      WHEN f.country IN ('US','IN','CA','GB','DE','AU','BR','FR') THEN f.country
      ELSE 'unknown'
    END AS country_std,
    (f.device_type NOT IN ('desktop','mobile','tablet')) AS dq_bad_device,
    (f.country NOT IN ('US','IN','CA','GB','DE','AU','BR','FR')) AS dq_bad_country
  FROM final_ts f
),

-- 7) Filter out internal users
filtered AS (
  SELECT s.*
  FROM standardized s
  JOIN core.dim_users u
    ON u.user_id = s.user_id
  WHERE COALESCE(u.is_internal, false) = false
)

SELECT
  event_id,
  user_id,
  event_name,
  event_time_final,
  feature_name,
  session_id,
  device_type_std AS device_type,
  country_std AS country,
  properties_json,
  ingested_at,

  -- DQ flags
  dq_missing_event_time,
  dq_used_client_time,
  dq_used_ingested_time,
  dq_duplicate_event_id,
  dq_bad_device,
  dq_bad_country

FROM filtered;

-- Helpful indexes for downstream analytics
CREATE INDEX IF NOT EXISTS idx_fct_events_user_time
  ON core.fct_events (user_id, event_time_final);

CREATE INDEX IF NOT EXISTS idx_fct_events_name_time
  ON core.fct_events (event_name, event_time_final);

CREATE INDEX IF NOT EXISTS idx_fct_events_feature_time
  ON core.fct_events (feature_name, event_time_final);
