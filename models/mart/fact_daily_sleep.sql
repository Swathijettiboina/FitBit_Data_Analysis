{{ config(materialized='table') }}

SELECT
    a.USER_ID,
    CAST(a.ACTIVITY_DAY AS DATE) AS ACTIVITY_DATE,
    u.USER_ID AS user_id,
    c.DATE_KEY AS activity_date_key,
    a.TOTAL_SLEEP_RECORDS,
    a.TOTAL_MINUTES_ASLEEP,
    a.TOTAL_TIME_IN_BED,
    a.SLEEP_EFFICIENCY
FROM {{ ref('int_daily_sleep_activity') }} a
JOIN {{ ref('dim_users') }} u ON a.USER_ID = u.USER_ID
JOIN {{ ref('dim_calendar') }} c ON CAST(a.ACTIVITY_DAY AS DATE) = c.DATE
