{{ config(
    materialized='table',
    tags=['mart', 'fact_daily_activity', 'fact'],
    description="This table contains daily activity data for each user, including steps, calories burned, and distance traveled.",
    unique_key='user_id'
) }}

WITH deduplicated AS (
    SELECT
        u.user_id,
        c.date AS activity_date,
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
        da.very_active_percent,
        da.moderate_activity_percent,
        da.light_activity_percent,
        s.step_count_level_id,
        calorie.calorie_level_id,
        ROW_NUMBER() OVER (PARTITION BY da.user_id, da.activity_date ORDER BY da.user_id) AS row_num
    FROM 
        {{ ref('int_daily_activity') }} da
    LEFT JOIN 
        {{ ref('dim_users') }} u 
        ON da.user_id = u.user_id
    LEFT JOIN 
        {{ ref('dim_calendar') }} c 
        ON da.activity_date = c.date
    LEFT JOIN 
        {{ ref('dim_step_count_level') }} s 
        ON da.step_count_level = s.step_count_level
    LEFT JOIN 
        {{ ref('dim_calorie_burn_level') }} calorie 
        ON da.calorie_burner_level = calorie.calorie_burn_level
)

SELECT 
    user_id, 
    activity_date,
    total_steps, 
    total_distance_km, 
    tracker_distance_km, 
    logged_activities_distance_km, 
    very_active_distance_km, 
    moderate_activity_distance_km,
    light_activity_distance_km, 
    sedentary_activity_distance_km, 
    very_active_minutes,
    moderate_activity_minutes, 
    light_activity_minutes, 
    sedentary_minutes, 
    calories_burned,
    very_active_percent, 
    moderate_activity_percent, 
    light_activity_percent, 
    step_count_level_id, 
    calorie_level_id
FROM deduplicated
WHERE row_num = 1
