{{
    config(
        materialized='table',
        tags=['core', 'int_daily_sleep_activity', 'physical'],
        description="This table contains daily sleep activity data from Fitbit devices, and it has sleep quality metrics."
    )
}}

WITH sleep_day AS (
    SELECT
        id                          AS user_id,
        DATE_TRUNC('day', sleepday)  AS activity_day,
        totalsleeprecords            AS total_sleep_records,
        totalminutesasleep           AS total_minutes_asleep,
        totaltimeinbed               AS total_time_in_bed
    FROM {{ ref('stg_sleepday') }}
)

SELECT
    user_id,
    activity_day,
    total_sleep_records,
    total_minutes_asleep,
    total_time_in_bed,
    CASE
        WHEN total_time_in_bed > 0 THEN
            total_minutes_asleep*100 / total_time_in_bed
        ELSE
            NULL
    END AS sleep_efficiency
FROM sleep_day
