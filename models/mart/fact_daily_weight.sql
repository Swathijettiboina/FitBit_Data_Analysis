{{ config(materialized='table') }}

SELECT
    a.USER_ID,
    a.RECORD_DATE,
    u.USER_ID AS user_id,
    c.DATE_KEY AS activity_date_key,
    a.AVG_WEIGHT_KG,
    a.AVG_BMI_SCORE,
    hs.health_status_id AS health_status_id,
    a.WEIGHT_TREND,
    a.DAYS_SINCE_PREVIOUS,
    a.PREV_WEIGHT,
    a.PREV_DATE
FROM {{ ref('int_daily_weight_metrics') }} a
JOIN {{ ref('dim_users') }} u ON a.USER_ID = u.USER_ID
JOIN {{ ref('dim_calendar') }} c ON a.RECORD_DATE = c.DATE
JOIN {{ ref('dim_health_status') }} hs ON a.HEALTH_STATUS = hs.health_status_name
