{{ config(materialized='table') }}

SELECT
    a.USER_ID,
    a.ACTIVITY_DATE,
    u.USER_ID AS user_id,
    c.DATE_KEY AS activity_date_key,
    a.TOTAL_STEPS,
    a.TOTAL_DISTANCE_KM,
    a.TRACKER_DISTANCE_KM,
    a.LOGGED_ACTIVITIES_DISTANCE_KM,
    a.VERY_ACTIVE_DISTANCE_KM,
    a.MODERATE_ACTIVITY_DISTANCE_KM,
    a.LIGHT_ACTIVITY_DISTANCE_KM,
    a.SEDENTARY_ACTIVITY_DISTANCE_KM,
    a.VERY_ACTIVE_MINUTES,
    a.MODERATE_ACTIVITY_MINUTES,
    a.LIGHT_ACTIVITY_MINUTES,
    a.SEDENTARY_MINUTES,
    a.CALORIES_BURNED,
    a.VERY_ACTIVE_PERCENT,
    a.MODERATE_ACTIVITY_PERCENT,
    a.LIGHT_ACTIVITY_PERCENT,
    a.STEPS_PER_KM,
    a.STEP_BASED_ACTIVITY_LEVEL,
    a.WORKOUT_INTENSITY,
    a.CALORIES_PER_KM,
    a.TOTAL_ACTIVE_MINUTES
FROM {{ ref('int_daily_activity') }} a
JOIN {{ ref('dim_users') }} u ON a.USER_ID = u.USER_ID
JOIN {{ ref('dim_calendar') }} c ON a.ACTIVITY_DATE = c.DATE
