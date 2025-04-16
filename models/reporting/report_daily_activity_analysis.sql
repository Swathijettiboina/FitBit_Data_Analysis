{{ 
    config(
        materialized='table',
        tags=['reporting', 'reporting_daily_activity', 'sleep_weight_analysis'],
        description="Comprehensive report combining daily activity, sleep activity, and weight metrics for users."
    )
}}

WITH unique_activity_data AS (
    SELECT DISTINCT
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
        act.step_count_level_id,
        act.calorie_level_id,
        phys.total_calories,
        phys.lightly_active_minutes,
        phys.fairly_active_minutes,
        phys.very_active_minutes,
        phys.sedentary_minutes,
        phys.active_minutes_total,
        phys.calories_per_step,
        phys.personal_activity_tag_id
    FROM {{ ref('fact_daily_activity') }} act
    LEFT JOIN {{ ref('fact_daily_physical_metrics') }} phys 
        ON act.user_id = phys.user_id AND act.activity_date = phys.activity_date
),
unique_sleep_weight_data AS (
    SELECT DISTINCT
        sa.user_id,
        sa.activity_date,
        sa.total_sleep_records,
        sa.total_minutes_asleep,
        sa.total_time_in_bed,
        sa.sleep_efficiency,
        wgt.health_status_id,
        wgt.avg_weight_kg,
        wgt.avg_bmi_score,
        wgt.weight_trend,
        wgt.days_since_previous,
        wgt.prev_weight
    FROM {{ ref('fact_daily_sleep_activity') }} sa
    LEFT JOIN {{ ref('fact_daily_weight_metrics') }} wgt 
        ON sa.user_id = wgt.user_id AND sa.activity_date = wgt.activity_date
)

SELECT
    uad.user_id,
    uad.activity_date,
    
    -- Activity Metrics
    uad.total_steps,
    uad.total_distance_km,
    uad.tracker_distance_km,
    uad.logged_activities_distance_km,
    uad.very_active_distance_km,
    uad.moderate_activity_distance_km,
    uad.light_activity_distance_km,
    uad.sedentary_activity_distance_km,
    uad.moderate_activity_minutes,
    uad.light_activity_minutes,
    uad.calories_burned,
    uad.very_active_percent,
    uad.moderate_activity_percent,
    uad.light_activity_percent,
    
    -- Physical Metrics
    COALESCE(step.step_count_level, 'No record') AS step_count_level,
    COALESCE(calorie.calorie_burn_level, 'No record') AS calorie_burn_level,
    COALESCE(pat.personal_activity_tag, 'No record') AS personal_activity_tag,
    COALESCE(uad.total_calories, 0) AS total_calories_burned,
    COALESCE(uad.lightly_active_minutes, 0) AS lightly_active_minutes,
    COALESCE(uad.fairly_active_minutes, 0) AS fairly_active_minutes,
    COALESCE(uad.very_active_minutes, 0) AS very_active_minutes,
    COALESCE(uad.sedentary_minutes, 0) AS sedentary_minutes,
    COALESCE(uad.active_minutes_total, 0) AS active_minutes_total,
    COALESCE(uad.calories_per_step, 0) AS calories_per_step,

    -- Sleep & Weight Metrics
    COALESCE(ss.total_sleep_records, 0) AS total_sleep_records,
    COALESCE(ss.total_minutes_asleep, 0) AS total_minutes_asleep,
    COALESCE(ss.total_time_in_bed, 0) AS total_time_in_bed,
    COALESCE(ss.sleep_efficiency, 0) AS sleep_efficiency,
    COALESCE(hs.health_status, 'No record') AS health_status,
    COALESCE(ss.avg_weight_kg, 0) AS avg_weight_kg,
    COALESCE(ss.avg_bmi_score, 0) AS avg_bmi_score,
    COALESCE(ss.weight_trend, 'No trend') AS weight_trend,
    COALESCE(ss.days_since_previous, 0) AS days_since_previous,
    COALESCE(ss.prev_weight, 0) AS prev_weight

FROM unique_activity_data uad
LEFT JOIN {{ ref('dim_step_count_level') }} step
    ON uad.step_count_level_id = step.step_count_level_id
LEFT JOIN {{ ref('dim_calorie_burn_level') }} calorie
    ON uad.calorie_level_id = calorie.calorie_level_id
LEFT JOIN {{ ref('dim_personal_activity_tag') }} pat
    ON uad.personal_activity_tag_id = pat.personal_activity_tag_id

-- Join with the sleep and weight data
LEFT JOIN unique_sleep_weight_data ss
    ON uad.user_id = ss.user_id AND uad.activity_date = ss.activity_date
LEFT JOIN {{ ref('dim_health_status') }} hs
    ON ss.health_status_id = hs.health_status_id
