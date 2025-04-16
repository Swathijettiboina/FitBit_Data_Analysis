{{ config(
    materialized='table',
    tags=['int', 'fact', 'fact_minute_metrics', 'mart', 'fact_heart_rate', 'analysis'],
    description="This table contains minute-level heart rate analysis data from Fitbit devices."
) }}

WITH deduplicated AS (
    SELECT
        hra.user_id,
        c.minute_timestamp AS activity_minute,
        hra.steps,
        hra.calories,
        hra.avg_heart_rate,
        hra.min_heart_rate,
        hra.max_heart_rate,
        hra.heart_rate_readings,
        hrz.heart_rate_zone_id,
        hra.calories_burnerd_per_step,
        ROW_NUMBER() OVER (PARTITION BY hra.user_id, c.minute_timestamp ORDER BY hra.user_id) AS row_num
    FROM 
        {{ ref('int_minute_heart_rate_analysis') }} hra
    LEFT JOIN 
        {{ ref('dim_users') }} u 
        ON hra.user_id = u.user_id
    LEFT JOIN 
        {{ ref('dim_calendar') }} c
        ON hra.activity_minute = c.minute_timestamp
    LEFT JOIN 
        {{ ref('dim_heart_rate_zone') }} hrz
        ON hra.heart_rate_zone = hrz.heart_rate_zone
)

SELECT
    user_id,
    activity_minute,
    steps,
    calories,
    avg_heart_rate,
    min_heart_rate,
    max_heart_rate,
    heart_rate_readings,
    heart_rate_zone_id,
    calories_burnerd_per_step
FROM deduplicated
WHERE row_num = 1