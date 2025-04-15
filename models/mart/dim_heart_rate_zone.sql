{{ config(materialized='table') }}

SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY TRIM(heart_rate_zone)) AS heart_rate_zone_id,
    TRIM(heart_rate_zone) AS heart_rate_zone
FROM {{ ref('int_minute_heart_rate_analysis') }}
WHERE heart_rate_zone IS NOT NULL
