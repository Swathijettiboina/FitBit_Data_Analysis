{{
    config(
        materialized='table',
        tags=['mart', 'fact_minute_activity'],
        description='Minute-level fact table aggregating Fitbit activity data for users.'
    )
}}

WITH user_calendar AS (
    SELECT 
        u.user_id,
        c.minute_timestamp
    FROM {{ ref('dim_users') }} AS u
    CROSS JOIN {{ ref('dim_calendar') }} AS c
),
minute_data AS (
    SELECT 
        uc.user_id,
        uc.minute_timestamp,
        COALESCE(fc.CALORIES, 0)                AS total_calories,
        COALESCE(fs.STEPS, 0)                   AS total_steps,
        COALESCE(fi.INTENSITY, 0)               AS total_intensity,
        COALESCE(fhr.VALUE, 0)                  AS heart_rate,
        COALESCE(fsleep.VALUE, 0)               AS total_sleep
    FROM user_calendar AS uc
    LEFT JOIN {{ ref('stg_minutecalories') }}            AS fc
        ON uc.user_id = fc.ID
        AND uc.minute_timestamp = fc.ACTIVITYMINUTE
    LEFT JOIN {{ ref('stg_minutestepsnarrow') }}         AS fs
        ON uc.user_id = fs.ID
        AND uc.minute_timestamp = fs.ACTIVITYMINUTE
    LEFT JOIN {{ ref('stg_minuteintensitiesnarrow') }}   AS fi
        ON uc.user_id = fi.ID
        AND uc.minute_timestamp = fi.ACTIVITYMINUTE
    LEFT JOIN {{ ref('stg_heartrateseconds') }}          AS fhr
        ON uc.user_id = fhr.ID
        AND uc.minute_timestamp = fhr.TIME
    LEFT JOIN {{ ref('stg_minutesleep') }}               AS fsleep
        ON uc.user_id = fsleep.ID
        AND uc.minute_timestamp = fsleep.DATE
),
aggregated_minute_data AS (
    SELECT 
        user_id,
        minute_timestamp,
        SUM(total_calories)             AS total_calories,
        SUM(total_steps)                AS total_steps,
        SUM(total_intensity)            AS total_intensity,
        AVG(heart_rate)                 AS avg_heart_rate,
        SUM(total_sleep)                AS total_sleep
    FROM minute_data
    GROUP BY user_id, minute_timestamp
)

SELECT 
    user_id,
    minute_timestamp,
    total_calories,
    total_steps,
    total_intensity,
    avg_heart_rate,
    total_sleep
FROM aggregated_minute_data
