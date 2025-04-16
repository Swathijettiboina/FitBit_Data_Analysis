{{ config(
    materialized='table',
    unique_key='id',
    tags=['fact_daily_sleep_activity', 'mart', 'daily', 'sleep', 'activity'],
    description="This table contains daily sleep activity data from Fitbit devices, and it has sleep quality metrics."
) }}

WITH deduplicated AS (
    SELECT
        u.user_id,
        c.date AS activity_date,
        sa.total_sleep_records,
        sa.total_minutes_asleep,
        sa.total_time_in_bed,
        sa.sleep_efficiency,
        ROW_NUMBER() OVER (PARTITION BY sa.user_id, sa.activity_day ORDER BY sa.user_id) AS row_num
    FROM 
        {{ ref('int_daily_sleep_activity') }} sa
    LEFT JOIN 
        {{ ref('dim_users') }} u ON sa.user_id = u.user_id
    LEFT JOIN 
        {{ ref('dim_calendar') }} c ON sa.activity_day = c.date
)

SELECT
    user_id,
    activity_date,
    total_sleep_records,
    total_minutes_asleep,
    total_time_in_bed,
    sleep_efficiency
FROM deduplicated
WHERE row_num = 1