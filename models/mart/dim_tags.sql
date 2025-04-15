{{ config(materialized='table') }}

SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY tag_value) AS tag_id,
    tag_value
FROM (
    SELECT calorie_burn_tag AS tag_value FROM {{ ref('int_hourly_metrics') }}
    UNION
    SELECT step_activity_tag FROM {{ ref('int_hourly_metrics') }}
    UNION
    SELECT intensity_tag FROM {{ ref('int_hourly_metrics') }}
    UNION
    SELECT personal_activity_tag FROM {{ ref('int_hourly_metrics') }}
)
WHERE tag_value IS NOT NULL
