{{
    config(
        materialized='table',
        tags=['fact_hourly_metrics', 'fact','mart','metrics'],
        description="Fact table containing hourly aggregated fitness metrics including calories burned, activity intensity, and step counts. Joined with dimension tables for classification levels.",
    )
}}

SELECT
    u.user_id,
    c.hour_timestamp AS activity_hour,
    hm.avg_calories,
    hm.avg_intensity,
    hm.avg_steps,
    cbl.calorie_level_id,
    scl.step_count_level_id,
    il.intensity_level_id,
    pat.personal_activity_tag_id
FROM 
    {{ ref('int_hourly_metrics') }} hm
JOIN 
    {{ ref('dim_users') }} u 
    ON hm.user_id = u.user_id
JOIN 
    {{ ref('dim_calendar') }} c 
    ON hm.activity_hour = c.hour_timestamp
JOIN 
    {{ ref('dim_calorie_burn_level') }} cbl 
    ON hm.calorie_burner_level = cbl.calorie_burn_level
JOIN 
    {{ ref('dim_step_count_level') }} scl 
    ON hm.step_count_level = scl.step_count_level
JOIN 
    {{ ref('dim_intensity_level') }} il 
    ON hm.intensity_level = il.intensity_level
JOIN 
    {{ ref('dim_personal_activity_tag') }} pat 
    ON hm.personal_activity_tag = pat.personal_activity_tag