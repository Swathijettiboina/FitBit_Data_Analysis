{{
  config(
    materialized='table',
    tags=['daily_activity', 'metrics','core_daily'],
  )
}}

WITH daily_activity AS (
    SELECT
        id                          AS user_id,
        activitydate                AS activity_date,
        totalsteps                  AS total_steps,
        totaldistance               AS total_distance_km,
        trackerdistance             AS tracker_distance_km,
        loggedactivitiesdistance    AS logged_activities_distance_km,
        veryactivedistance          AS very_active_distance_km,
        moderatelyactivedistance    AS moderate_activity_distance_km,
        lightactivedistance         AS light_activity_distance_km,
        sedentaryactivedistance     AS sedentary_activity_distance_km,
        veryactiveminutes           AS very_active_minutes,
        fairlyactiveminutes         AS moderate_activity_minutes,
        lightlyactiveminutes        AS light_activity_minutes,
        sedentaryminutes            AS sedentary_minutes,
        calories                    AS calories_burned
    FROM {{ ref('stg_dailyactivity') }}
)

SELECT
    -- Original Columns
    da.user_id,
    da.activity_date,
    da.total_steps,
    da.total_distance_km,
    da.tracker_distance_km,
    da.logged_activities_distance_km,
    da.very_active_distance_km,
    da.moderate_activity_distance_km,
    da.light_activity_distance_km,
    da.sedentary_activity_distance_km,
    da.very_active_minutes,
    da.moderate_activity_minutes,
    da.light_activity_minutes,
    da.sedentary_minutes,
    da.calories_burned,
    
    COALESCE(ROUND(da.very_active_minutes * 100.0 / NULLIF(
        da.very_active_minutes + 
        da.moderate_activity_minutes + 
        da.light_activity_minutes + 
        da.sedentary_minutes, 0), 1), 0) AS very_active_percent,
    COALESCE(ROUND(da.moderate_activity_minutes * 100.0 / NULLIF(
        da.very_active_minutes + 
        da.moderate_activity_minutes + 
        da.light_activity_minutes + 
        da.sedentary_minutes, 0), 1), 0) AS moderate_activity_percent,
    COALESCE(ROUND(da.light_activity_minutes * 100.0 / NULLIF(
        da.very_active_minutes + 
        da.moderate_activity_minutes + 
        da.light_activity_minutes + 
        da.sedentary_minutes, 0), 1), 0) AS light_activity_percent,
    
    COALESCE(da.total_steps / NULLIF(da.total_distance_km, 0), 0) AS steps_per_km,
    CASE
        WHEN da.total_steps >= 10000 THEN 'Highly Active'
        WHEN da.total_steps >= 7500 THEN 'Moderately Active'
        WHEN da.total_steps >= 5000 THEN 'Lightly Active'
        ELSE 'Sedentary'
    END AS step_based_activity_level,
    
    CASE
        WHEN da.calories_burned >= 500 THEN 'Intense Workout'
        WHEN da.calories_burned >= 300 THEN 'Moderate Workout'
        WHEN da.calories_burned >= 100 THEN 'Light Activity'
        ELSE 'Minimal Activity'
    END AS workout_intensity,
    COALESCE(ROUND(da.calories_burned / NULLIF(da.total_distance_km, 0), 1), 0)                      AS calories_per_km,
    COALESCE((da.very_active_minutes + da.moderate_activity_minutes + da.light_activity_minutes), 0) AS total_active_minutes,

FROM daily_activity da