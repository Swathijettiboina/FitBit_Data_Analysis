{{ config(materialized='table') }}

SELECT
    a.USER_ID,
    a.ACTIVITY_DATE,
    u.USER_ID AS user_id,
    c.DATE_KEY AS activity_date_key,
    a.TOTAL_STEPS,
    a.TOTAL_CALORIES,
    a.LIGHTLY_ACTIVE_MINUTES,
    a.FAIRLY_ACTIVE_MINUTES,
    a.VERY_ACTIVE_MINUTES,
    a.SEDENTARY_MINUTES,
    a.ACTIVE_MINUTES_TOTAL,
    a.CALORIES_PER_STEP,
    al.activity_level_id AS activity_level_id
FROM {{ ref('int_daily_physical_metrics') }} a
JOIN {{ ref('dim_users') }} u ON a.USER_ID = u.USER_ID
JOIN {{ ref('dim_calendar') }} c ON a.ACTIVITY_DATE = c.DATE
JOIN {{ ref('dim_activity_level') }} al ON a.ACTIVITY_LEVEL = al.activity_level_name
