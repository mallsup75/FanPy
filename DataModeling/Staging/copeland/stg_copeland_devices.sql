--models/staging/copeland/stg_copeland__devices.sql

SELECT distinct

  a.deviceid,
  a.devicetype
 FROM {{ source('copeland_seeds', 'copeland_wide') }} a
