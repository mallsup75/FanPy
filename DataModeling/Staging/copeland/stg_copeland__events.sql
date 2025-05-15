--models/staging/copeland/stg_copeland__events.sql
{{
    config(
        materialized='incremental',
        unique_key='event_alarm_id',
        incremental_strategy='merge',
        schema='copeland_schema',
        on_schema_change='sync_all_columns'
    )
}}

WITH parsed_alarm AS (
  SELECT
    eventuhub_id,
    'a1' AS alarm_type,
    TIMESTAMP(TRIM(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(alarms, '$.a1'), r'[^0-9\-T:.]', ''))) AS alarm_timestamp,
    logged
  FROM {{ source('copeland_seeds', 'copeland_wide') }}
  WHERE JSON_EXTRACT_SCALAR(alarms, '$.a1') IS NOT NULL

  UNION ALL

  SELECT
    eventuhub_id,
    'a2' AS alarm_type,
    TIMESTAMP(TRIM(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(alarms, '$.a2'), r'[^0-9\-T:.]', ''))) AS alarm_timestamp,
    logged
  FROM {{ source('copeland_seeds', 'copeland_wide') }}
  WHERE JSON_EXTRACT_SCALAR(alarms, '$.a2') IS NOT NULL

  UNION ALL

  SELECT
    eventuhub_id,
    'a3' AS alarm_type,
    TIMESTAMP(TRIM(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(alarms, '$.a3'), r'[^0-9\-T:.]', ''))) AS alarm_timestamp,
    logged
  FROM {{ source('copeland_seeds', 'copeland_wide') }}
  WHERE JSON_EXTRACT_SCALAR(alarms, '$.a3') IS NOT NULL
)

SELECT
  a.eventuhub_id,
  CONCAT(a.eventuhub_id, '-', STA.alarm_type) AS event_alarm_id,
  a.companies,
  a.deviceid,
  STA.logged,
  STA.alarm_type,
  STA.alarm_timestamp,
  TIMESTAMP_DIFF(STA.alarm_timestamp, TIMESTAMP(STA.logged), MINUTE) AS alarm_log_minutes_diff,
  a.p1,
  a.p2,
  a.p3,
  a.p4,
  a.p5,
  a.p6,
  a.p7,
  a.p8,
  a.p9,
  a.p10
FROM parsed_alarm STA
JOIN {{ source('copeland_seeds', 'copeland_wide') }} a
  ON a.eventuhub_id = STA.eventuhub_id
WHERE STA.alarm_timestamp IS NOT NULL
  AND STA.logged IS NOT NULL
{% if is_incremental() %}
  AND (a.logged > (SELECT COALESCE(MAX(logged), '1900-01-01') FROM {{ this }}) OR a.logged IS NULL)
{% endif %}
