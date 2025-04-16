{{
    config(
        materialized='table',
        tags=['mart', 'reporting', 'daily_activity'],
        description="Comprehensive daily activity reporting table combining physical activity, sleep, and weight metrics for each user.",
        unique_key=['user_id', 'activity_date']
    )
}}
SELECT
    act.user_id,
    act.activity_date,
    act.total_steps,
    act.total_distance_km,
    act.tracker_distance_km,
    act.logged_activities_distance_km,
    act.very_active_distance_km,
    act.moderate_activity_distance_km,
    act.light_activity_distance_km,
    act.sedentary_activity_distance_km,
    act.moderate_activity_minutes,
    act.light_activity_minutes,
    act.calories_burned,
    act.very_active_percent,
    act.moderate_activity_percent,
    act.light_activity_percent,

    COALESCE(step.step_count_level, 'No record') AS step_count_level,
    COALESCE(calorie.calorie_burn_level, 'No record') AS calorie_burn_level,
    COALESCE(pat.personal_activity_tag, 'No record') AS personal_activity_tag,

    COALESCE(phys.total_calories, 0) AS total_calories_burned,
    COALESCE(phys.lightly_active_minutes, 0) AS lightly_active_minutes,
    COALESCE(phys.fairly_active_minutes, 0) AS fairly_active_minutes,
    COALESCE(phys.very_active_minutes, 0) AS very_active_minutes,
    COALESCE(phys.sedentary_minutes, 0) AS sedentary_minutes,
    COALESCE(phys.active_minutes_total, 0) AS active_minutes_total,
    COALESCE(phys.calories_per_step, 0) AS calories_per_step,
    
    

FROM {{ ref('fact_daily_activity') }} act
LEFT JOIN 
    {{ ref('fact_daily_physical_metrics') }} phys 
    ON act.user_id = phys.user_id AND act.activity_date = phys.activity_date

LEFT JOIN
    {{ ref('dim_step_count_level') }} step
    ON act.step_count_level_id = step.step_count_level_id
LEFT JOIN
    {{ ref('dim_calorie_burn_level') }} calorie
    ON act.calorie_level_id = calorie.calorie_level_id
LEFT JOIN
    {{ ref('dim_personal_activity_tag') }} pat
    ON phys.personal_activity_tag_id = pat.personal_activity_tag_id