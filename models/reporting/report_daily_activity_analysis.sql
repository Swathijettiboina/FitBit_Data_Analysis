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
    COALESCE(hs.health_status, 'No record') AS health_status,

    -- Physical metrics from fact_daily_physical_metrics
    phys.total_calories                     AS total_calories_burned,
    phys.lightly_active_minutes,
    phys.fairly_active_minutes,
    phys.very_active_minutes                AS very_active_minutes,
    phys.sedentary_minutes                  AS sedentary_minutes,
    phys.active_minutes_total,
    phys.calories_per_step,
    
    -- Sleep metrics from fact_daily_sleep_activity
    sleep.total_minutes_asleep,
    sleep.total_time_in_bed,
    sleep.sleep_efficiency,
    
    -- Weight metrics from fact_daily_weight_metrics
    wgt.avg_weight_kg,
    wgt.avg_bmi_score,
    wgt.weight_trend,
    wgt.days_since_previous,
    wgt.prev_weight

FROM {{ ref('fact_daily_activity') }} act
JOIN 
    {{ ref('fact_daily_physical_metrics') }} phys 
    ON act.user_id = phys.user_id AND act.activity_date = phys.activity_date
JOIN 
    {{ ref('fact_daily_sleep_activity') }} sleep
    ON act.user_id = sleep.user_id 
    AND act.activity_date = sleep.activity_date
JOIN 
    {{ ref('fact_daily_weight_metrics') }} wgt
    ON act.user_id = wgt.user_id 
    AND act.activity_date = wgt.activity_date
JOIN
    {{ ref('dim_step_count_level') }} step
    ON act.step_count_level_id = step.step_count_level_id
JOIN
    {{ ref('dim_calorie_burn_level') }} calorie
    ON act.calorie_level_id = calorie.calorie_level_id
JOIN
    {{ ref('dim_personal_activity_tag') }} pat
    ON phys.personal_activity_tag_id = pat.personal_activity_tag_id
JOIN
    {{ ref('dim_health_status') }} hs
    ON wgt.health_status_id = hs.health_status_id