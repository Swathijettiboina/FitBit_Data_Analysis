{{ config(materialized='table') }}

SELECT
    a.USER_ID,
    a.ACTIVITY_MINUTE,
    u.USER_ID AS user_id,
    c.DATE_KEY AS activity_date_key,
    t.HOUR_OF_DAY AS activity_hour,
    t.MINUTE_OF_HOUR AS activity_minute,
    a.STEPS,
    a.CALORIES,
    a.AVG_HEART_RATE,
    a.MIN_HEART_RATE,
    a.MAX_HEART_RATE,
    a.HEART_RATE_READINGS,
    hrz.heart_rate_zone_id AS heart_rate_zone_id,
    a.CALORIES_BURNERD_PER_STEP
FROM {{ ref('int_minute_heart_rate_analysis') }} a
JOIN {{ ref('dim_users') }} u ON a.USER_ID = u.USER_ID
JOIN {{ ref('dim_calendar') }} c ON CAST(a.ACTIVITY_MINUTE AS DATE) = c.DATE
JOIN {{ ref('dim_time') }} t ON EXTRACT(HOUR FROM a.ACTIVITY_MINUTE) = t.HOUR_OF_DAY
JOIN {{ ref('dim_heart_rate_zone') }} hrz ON a.HEART_RATE_ZONE = hrz.heart_rate_zone
