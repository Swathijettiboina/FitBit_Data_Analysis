{{ config(materialized='table') }}

SELECT
    a.USER_ID,
    a.ACTIVITY_HOUR,
    u.USER_ID AS user_id,
    c.DATE_KEY AS activity_date_key,
    t.HOUR_OF_DAY AS activity_hour,
    a.AVG_CALORIES,
    a.AVG_INTENSITY,
    a.AVG_STEPS,
    a.CALORIE_BURN_TAG,
    a.STEP_ACTIVITY_TAG,
    a.INTENSITY_TAG,
    a.PERSONAL_ACTIVITY_TAG
FROM {{ ref('int_hourly_metrics') }} a
JOIN {{ ref('dim_users') }} u ON a.USER_ID = u.USER_ID
JOIN {{ ref('dim_calendar') }} c ON CAST(a.ACTIVITY_HOUR AS DATE) = c.DATE
JOIN {{ ref('dim_time') }} t ON EXTRACT(HOUR FROM a.ACTIVITY_HOUR) = t.HOUR_OF_DAY
