{{
    config(
        materialized='incremental',
        unique_key='event_company_id',
        incremental_strategy='merge',
        schema='copeland_schema',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
  t.eventuhub_id,
  JSON_VALUE(company, '$.Id') AS companyid,
  CONCAT(t.eventuhub_id, '-', JSON_VALUE(company, '$.Id')) AS event_company_id,
  TIMESTAMP(t.logged) AS logged
FROM {{ source('copeland_seeds', 'copeland_wide') }} t
CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(t.companies)) AS company
WHERE JSON_VALUE(company, '$.Id') IS NOT NULL
{% if is_incremental() %}
  AND (t.updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }}) OR t.updated_at IS NULL)
{% endif %}
