{{ config(
    materialized='table',
    unique_key='id',
    tags=['daily_weight_metrics', 'fact', 'weight', 'mart', 'metrics'],
    description="This table contains daily weight metrics for users, including weight, BMI."
) }}

WITH deduplicated AS (
    SELECT
        u.user_id,
        c.date AS activity_date,
        wm.avg_weight_kg,
        wm.avg_bmi_score,
        hs.health_status_id,
        wm.weight_trend,
        wm.days_since_previous,
        wm.prev_weight,
        wm.prev_date,
        ROW_NUMBER() OVER (PARTITION BY wm.user_id, wm.record_date ORDER BY wm.user_id) AS row_num
    FROM 
        {{ ref('int_daily_weight_metrics') }} wm
    LEFT JOIN 
        {{ ref('dim_users') }} u ON wm.user_id = u.user_id
    LEFT JOIN 
        {{ ref('dim_calendar') }} c ON wm.record_date = c.date
    LEFT JOIN 
        {{ ref('dim_health_status') }} hs ON wm.health_status = hs.health_status
)

SELECT
    user_id,
    activity_date,
    avg_weight_kg,
    avg_bmi_score,
    health_status_id,
    weight_trend,
    days_since_previous,
    prev_weight,
    prev_date
FROM deduplicated
WHERE row_num = 1
