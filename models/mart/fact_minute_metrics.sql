{{ config(materialized='table') }}

SELECT
    a.USER_ID,
    a.ACTIVITY_MINUTE,
    u.USER_ID AS user_id,
    c.DATE_KEY AS activity_date_key,
    t.HOUR_OF_DAY AS activity_hour,
    t.MINUTE_OF_HOUR AS activity_minute,
    a.AVG_CALORIES,
    a.AVG_INTENSITY,
    a.AVG_METS,
    a.AVG_STEPS,
    a.CALORIE_BURN_TAG,
    i.intensity_id AS intensity_id,
    a.STEP_ACTIVITY_TAG,
    a.PERSONAL_ACTIVITY_TAG
FROM {{ ref('int_minute_metrics') }} a
JOIN {{ ref('dim_users') }} u ON a.USER_ID = u.USER_ID
JOIN {{ ref('dim_calendar') }} c ON CAST(a.ACTIVITY_MINUTE AS DATE) = c.DATE
JOIN {{ ref('dim_time') }} t ON EXTRACT(HOUR FROM a.ACTIVITY_MINUTE) = t.HOUR_OF_DAY
JOIN {{ ref('dim_intensity') }} i ON a.INTENSITY_TAG = i.intensity_level
