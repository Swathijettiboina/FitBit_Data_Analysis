{{
    config(
        materialized='table',
        tags=['fact_minute_metrics','mart','fact', 'metrics'],
        description="This table contains minute-level metrics data from Fitbit, including calories burned, intensity, METs, and step counts."
    )
}}

SELECT
    u.user_id,
    c.minute_timestamp AS activity_minute,
    mm.avg_calories,
    mm.avg_intensity,
    mm.avg_mets,
    mm.avg_steps,
    cbl.calorie_level_id,
    scl.step_count_level_id,
    il.intensity_level_id,
    pat.personal_activity_tag_id
FROM 
    {{ ref('int_minute_metrics') }} mm
JOIN 
    {{ ref('dim_users') }} u 
    ON mm.user_id = u.user_id
JOIN 
    {{ ref('dim_calendar') }} c 
    ON mm.activity_minute = c.minute_timestamp
JOIN 
    {{ ref('dim_calorie_burn_level') }} cbl 
    ON mm.calorie_burner_level = cbl.calorie_burn_level
JOIN 
    {{ ref('dim_step_count_level') }} scl 
    ON mm.step_count_level = scl.step_count_level
JOIN 
    {{ ref('dim_intensity_level') }} il 
    ON mm.intensity_level = il.intensity_level
JOIN 
    {{ ref('dim_personal_activity_tag') }} pat 
    ON mm.personal_activity_tag = pat.personal_activity_tag