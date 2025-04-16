{{
    config(
        materialized='table',
        unique_key='id',
        tags=['fact','mart','daily', 'sleep', 'activity'],
        description="This table contains daily sleep activity data from Fitbit devices, and it has sleep quality metrics."
    )
}}
SELECT
    u.user_id,
    c.date AS activity_date,
    sa.total_sleep_records,
    sa.total_minutes_asleep,
    sa.total_time_in_bed,
    sa.sleep_efficiency
FROM {{ ref('int_daily_sleep_activity') }} sa
JOIN {{ ref('dim_users') }} u ON sa.user_id = u.user_id
JOIN {{ ref('dim_calendar') }} c ON sa.activity_day = c.date