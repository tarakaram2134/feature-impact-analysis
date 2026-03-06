-- =====================================================
-- 20_sessions.sql
-- Build canonical sessions using 30-min inactivity rule
-- =====================================================

DROP TABLE IF EXISTS core.fct_sessions;

CREATE TABLE core.fct_sessions AS
WITH ordered AS (
  SELECT
    e.*,
    LAG(e.event_time_final) OVER (
      PARTITION BY e.user_id
      ORDER BY e.event_time_final
    ) AS prev_event_time
  FROM core.fct_events e
),

session_flags AS (
  SELECT
    o.*,
    CASE
      WHEN o.prev_event_time IS NULL THEN 1
      WHEN o.event_time_final - o.prev_event_time > INTERVAL '30 minutes' THEN 1
      ELSE 0
    END AS is_new_session
  FROM ordered o
),

session_numbered AS (
  SELECT
    s.*,
    SUM(s.is_new_session) OVER (
      PARTITION BY s.user_id
      ORDER BY s.event_time_final
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_number
  FROM session_flags s
),

events_with_session_key AS (
  SELECT
    user_id,
    session_number,
    MIN(event_time_final) AS session_start_at,
    MAX(event_time_final) AS session_end_at,
    COUNT(*) AS event_count,

    -- activity counts (customize as needed)
    SUM((event_name = 'login')::int) AS login_events,
    SUM((event_name = 'page_view')::int) AS page_views,
    SUM((event_name = 'project_created')::int) AS projects_created,
    SUM((event_name = 'message_sent')::int) AS messages_sent,
    SUM((event_name = 'file_uploaded')::int) AS files_uploaded,

    -- feature usage in-session
    SUM((event_name = 'feature_exposed' AND feature_name = 'team_collab')::int) AS team_collab_exposed,
    SUM((event_name = 'feature_viewed'  AND feature_name = 'team_collab')::int) AS team_collab_viewed,
    SUM((event_name = 'feature_used'    AND feature_name = 'team_collab')::int) AS team_collab_used,

    -- keep some diagnostics about raw session ids
    COUNT(DISTINCT session_id) AS raw_session_id_count

  FROM session_numbered
  GROUP BY user_id, session_number
),

final AS (
  SELECT
    ews.user_id,
    -- stable session key for joins (human-readable)
    CONCAT(ews.user_id, '_s', LPAD(ews.session_number::text, 6, '0')) AS session_key,
    ews.session_number,
    ews.session_start_at,
    ews.session_end_at,
    EXTRACT(EPOCH FROM (ews.session_end_at - ews.session_start_at))::bigint AS session_duration_seconds,
    ews.event_count,

    ews.login_events,
    ews.page_views,
    ews.projects_created,
    ews.messages_sent,
    ews.files_uploaded,

    ews.team_collab_exposed,
    ews.team_collab_viewed,
    ews.team_collab_used,

    (ews.team_collab_used > 0) AS used_team_collab_in_session,
    ews.raw_session_id_count

  FROM events_with_session_key ews
)
SELECT * FROM final;

CREATE INDEX IF NOT EXISTS idx_fct_sessions_user_start
  ON core.fct_sessions (user_id, session_start_at);
