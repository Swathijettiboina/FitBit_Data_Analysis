{{ config(
    materialized='table',
    unique_key='id',
    tags=['fact', 'fact_daily', 'physical', 'metrics', 'dpm', 'mart'],
    description="This table contains daily physical metrics data from Fitbit."
) }}

WITH deduplicated AS (
    SELECT
        u.user_id,
        c.date AS activity_date,
        dp.total_steps,
        dp.total_calories,
        dp.lightly_active_minutes,
        dp.fairly_active_minutes,
        dp.very_active_minutes,
        dp.sedentary_minutes,
        dp.active_minutes_total,
        dp.calories_per_step,
        pat.personal_activity_tag_id,
        ROW_NUMBER() OVER (PARTITION BY dp.user_id, dp.activity_date ORDER BY dp.user_id) AS row_num
    FROM 
        {{ ref('int_daily_physical_metrics') }} dp
    LEFT JOIN 
        {{ ref('dim_users') }} u ON dp.user_id = u.user_id
    LEFT JOIN 
        {{ ref('dim_calendar') }} c ON dp.activity_date = c.date
    LEFT JOIN 
        {{ ref('dim_personal_activity_tag') }} pat ON dp.personal_activity_tag = pat.personal_activity_tag
)

SELECT 
    user_id,
    activity_date,
    total_steps,
    total_calories,
    lightly_active_minutes,
    fairly_active_minutes,
    very_active_minutes,
    sedentary_minutes,
    active_minutes_total,
    calories_per_step,
    personal_activity_tag_id
FROM deduplicated
WHERE row_num = 1
