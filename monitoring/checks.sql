-- ============================================================================
-- checks.sql: the alert rules (run by monitoring/alerts.py)
-- ============================================================================
-- Every rule answers with the same four columns:
--
--     severity     critical = somebody must act today, warn = look when you can
--     check_name   which rule fired
--     object       which table / dbt node / task
--     detail       the number that made it fire
--
-- Zero rows = all is well. Rules read ONLY the monitoring schema (what the
-- collectors recorded about the warehouse), never Bronze/Silver/Gold directly,
-- so the whole file is cheap enough to run several times a day.
--
-- "today" is the business day: alerts.py sets the session TimeZone to
-- ALERT_TIMEZONE (default Asia/Ulaanbaatar) before running this file.
--
-- Run by hand:
--   python -m monitoring.alerts --dry-run
-- ============================================================================

WITH dq_runs AS (
    SELECT run_id, checked_at,
           row_number() OVER (ORDER BY checked_at DESC) AS recency
    FROM monitoring.data_quality_runs
),
latest_dq AS (
    SELECT snapshot.*
    FROM monitoring.data_quality_snapshots AS snapshot
    JOIN dq_runs USING (run_id)
    WHERE dq_runs.recency = 1
),
previous_dq AS (
    SELECT snapshot.*
    FROM monitoring.data_quality_snapshots AS snapshot
    JOIN dq_runs USING (run_id)
    WHERE dq_runs.recency = 2
),
dbt_invocations AS (
    SELECT invocation_id,
           row_number() OVER (ORDER BY max(generated_at) DESC) AS recency
    FROM monitoring.dbt_node_results
    GROUP BY invocation_id
),
latest_dbt AS (
    SELECT results.*
    FROM monitoring.dbt_node_results AS results
    JOIN dbt_invocations USING (invocation_id)
    WHERE dbt_invocations.recency = 1
),
previous_dbt AS (
    SELECT results.*
    FROM monitoring.dbt_node_results AS results
    JOIN dbt_invocations USING (invocation_id)
    WHERE dbt_invocations.recency = 2
),
-- Final state of each task in each run: a failure followed by a successful retry is not a failure.
final_tries AS (
    SELECT DISTINCT ON (dag_id, task_id, run_id) *
    FROM monitoring.task_runs
    ORDER BY dag_id, task_id, run_id, try_number DESC
),
latest_reconciliation AS (
    SELECT *
    FROM monitoring.reconciliation
    WHERE checked_at = (SELECT max(checked_at) FROM monitoring.reconciliation)
)

-- 1 . FRESHNESS . a source stopped arriving --------------------------------
SELECT CASE freshness_status WHEN 'fail' THEN 'critical' ELSE 'warn' END AS severity,
       'stale_source' AS check_name,
       schema_name || '.' || table_name AS object,
       'last loaded ' || coalesce(to_char(last_ingested_at, 'YYYY-MM-DD HH24:MI'), 'never') AS detail
FROM latest_dq
WHERE freshness_status IN ('fail', 'warn')
  -- RAWG is optional: a never-loaded catalogue (no API key) is not an alert.
  AND NOT (table_name LIKE '%rawg%' AND row_count = 0)

UNION ALL

-- 2 . VOLUME . a required relation is empty ----------------------------------
SELECT 'critical', 'empty_relation', schema_name || '.' || table_name, '0 rows'
FROM latest_dq
WHERE row_count = 0 AND volume_status = 'fail'

UNION ALL

-- 3 . VOLUME . an unusually large jump (data_quality.py threshold) -------------
SELECT 'warn', 'volume_jump', schema_name || '.' || table_name,
       round(volume_change_percent, 1) || ' % since the previous check, now ' || row_count || ' rows'
FROM latest_dq
WHERE volume_status = 'warn' AND row_count > 0

UNION ALL

-- 4 . VOLUME . rows disappeared from a table that only ever grows -------------
-- Events and accounts are append-only at the source; a shrinking table was
-- truncated, overwritten or half-loaded.
SELECT 'critical', 'rows_disappeared', latest_dq.schema_name || '.' || latest_dq.table_name,
       (latest_dq.row_count - previous_dq.row_count) || ' rows since the previous check, now '
           || latest_dq.row_count
FROM latest_dq
JOIN previous_dq USING (schema_name, table_name)
WHERE latest_dq.row_count < previous_dq.row_count
  AND (latest_dq.schema_name, latest_dq.table_name) IN (
      ('bronze', 'players'), ('bronze', 'games'), ('bronze', 'sessions'), ('bronze', 'purchases'),
      ('bronze', 'achievements'), ('bronze', 'reviews'), ('bronze', 'business_record_history'),
      ('gold', 'dim_players'), ('gold', 'dim_games'), ('gold', 'fact_sessions'),
      ('gold', 'fact_purchases'), ('gold', 'fact_reviews'), ('gold', 'fact_achievements'),
      ('snapshots', 'snap_players'), ('snapshots', 'snap_games')
  )

UNION ALL

-- 5 . SCHEMA . a relation gained or lost columns -------------------------------
SELECT 'warn', 'schema_changed', latest_dq.schema_name || '.' || latest_dq.table_name,
       previous_dq.column_count || ' -> ' || latest_dq.column_count || ' columns'
FROM latest_dq
JOIN previous_dq USING (schema_name, table_name)
WHERE latest_dq.column_count IS DISTINCT FROM previous_dq.column_count
  AND latest_dq.column_count IS NOT NULL
  AND previous_dq.column_count IS NOT NULL

UNION ALL

-- 6 . QUALITY . a dbt model, snapshot, seed or test failed -----------------------
SELECT 'critical', 'dbt_failed', resource_type || ' . ' || node_name,
       status || CASE WHEN resource_type = 'test' AND failures IS NOT NULL
                      THEN ' . ' || failures || ' rows' ELSE '' END
FROM latest_dbt
WHERE status IN ('error', 'fail')

UNION ALL

-- 7 . QUALITY . nodes skipped because something upstream failed -----------------
SELECT 'warn', 'dbt_skipped', 'dbt build', count(*) || ' nodes skipped'
FROM latest_dbt
WHERE status = 'skipped'
HAVING count(*) > 0

UNION ALL

-- 8 . QUALITY . a warning is new or got worse ------------------------------------
-- A warning that says "71" every day is noise; "71 -> 140" is news.
SELECT 'warn', 'warning_grew', latest_dbt.node_name,
       coalesce(previous_dbt.failures::text, 'new') || ' -> ' || latest_dbt.failures || ' rows'
FROM latest_dbt
LEFT JOIN previous_dbt
       ON previous_dbt.unique_id = latest_dbt.unique_id AND previous_dbt.status = 'warn'
WHERE latest_dbt.status = 'warn'
  AND (previous_dbt.failures IS NULL OR latest_dbt.failures > previous_dbt.failures)

UNION ALL

-- 9 . RUNS . a task failed today (after its retries) and is still broken -----------
-- A failure that a later run of the same task fixed is history, not a problem: without
-- this, one red run in the morning keeps check_and_alert red for the rest of the day.
-- The alerting tasks themselves are left out (their failure IS the alert, and counting
-- it would make every later check_and_alert fail too), and so is the alert drill.
SELECT 'critical', 'task_failed', failed.dag_id || ' . ' || failed.task_id,
       'try ' || failed.try_number || ' . run ' || failed.run_id
FROM final_tries AS failed
WHERE failed.state = 'failed' AND failed.ended_at::date = current_date
  AND failed.dag_id <> 'alert_drill'
  AND failed.task_id NOT IN ('check_and_alert', 'watchdog')
  AND NOT EXISTS (
      SELECT 1 FROM final_tries AS later
      WHERE later.dag_id = failed.dag_id AND later.task_id = failed.task_id
        AND later.state = 'success' AND later.ended_at > failed.ended_at
  )

UNION ALL

-- 10 . RUNS . a task took 3x longer than its 7-day average --------------------------
SELECT 'warn', 'slow_task', today.dag_id || ' . ' || today.task_id,
       round(today.duration_seconds) || ' s, usually ' || round(history.usual) || ' s'
FROM (
    SELECT dag_id, task_id, max(duration_seconds) AS duration_seconds
    FROM final_tries
    WHERE state = 'success' AND ended_at::date = current_date
    GROUP BY dag_id, task_id
) AS today
JOIN (
    SELECT dag_id, task_id, avg(duration_seconds) AS usual
    FROM final_tries
    WHERE state = 'success'
      AND ended_at::date BETWEEN current_date - 7 AND current_date - 1
    GROUP BY dag_id, task_id
) AS history USING (dag_id, task_id)
WHERE today.duration_seconds > 60 AND today.duration_seconds > 3 * history.usual

UNION ALL

-- 11 . SILENCE . the daily pipeline did not check anything today ---------------------
-- Rules 1-10 read today's rows. If nothing ran, they say nothing, and silence looks
-- exactly like health. This rule (run by the watchdog DAG) tells the two apart.
SELECT 'critical', 'pipeline_silent', 'monitoring.data_quality_runs',
       'last quality check ' || coalesce(to_char(max(checked_at), 'YYYY-MM-DD HH24:MI'), 'never')
FROM monitoring.data_quality_runs
HAVING coalesce(max(checked_at)::date, DATE '1900-01-01') < current_date

UNION ALL

-- 12 . SILENCE . the hourly source generator stopped -----------------------------------
-- Only once the generator has run at least once (it is optional).
SELECT 'warn', 'generator_silent', 'gaming_source_data_generator',
       'last success ' || to_char(max(ended_at), 'YYYY-MM-DD HH24:MI')
FROM final_tries
WHERE dag_id = 'gaming_source_data_generator' AND state = 'success'
HAVING max(ended_at) < now() - interval '3 hours'

UNION ALL

-- 13 . COMPLETENESS . the source has rows that never reached Bronze -----------------------
SELECT 'critical', 'source_bronze_mismatch', 'bronze.' || table_name,
       CASE status WHEN 'bronze_empty' THEN 'Bronze empty, source has ' || source_rows || ' rows'
            ELSE (source_rows - bronze_rows) || ' rows missing (source ' || source_rows
                 || ', Bronze ' || bronze_rows || ' up to ' || to_char(compared_up_to, 'YYYY-MM-DD HH24:MI') || ')'
       END
FROM latest_reconciliation
WHERE status IN ('missing_in_bronze', 'bronze_empty')

UNION ALL

-- 14 . COMPLETENESS . Bronze keeps rows the source no longer has (hard deletes) ------------
SELECT 'warn', 'deleted_at_source', 'bronze.' || table_name,
       difference || ' more rows than the source: hard deletes are not replicated'
FROM latest_reconciliation
WHERE status = 'extra_in_bronze'
