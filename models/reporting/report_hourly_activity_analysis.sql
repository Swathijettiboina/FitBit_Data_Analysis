{{
    config  (
        materialized='table',
        unique_key='id',
        tags=['report', 'hourly', 'activity', 'rpt_hourly_analysis'],
        description="This table contains hourly activity analysis data from Fitbit devices."
    )
}}

SELECT
    hm.user_id,
    hm.activity_hour,
    hm.avg_calories,
    hm.avg_intensity,
    hm.avg_steps,
    cbl.calorie_burn_level,
    scl.step_count_level,
    il.intensity_level,
    pat.personal_activity_tag
FROM 
    {{ ref('fact_hourly_metrics') }} hm
JOIN 
    {{ ref('dim_calorie_burn_level') }} cbl
    ON hm.calorie_level_id = cbl.calorie_level_id
JOIN 
    {{ ref('dim_step_count_level') }} scl
    ON hm.step_count_level_id = scl.step_count_level_id
JOIN 
    {{ ref('dim_intensity_level') }} il
    ON hm.intensity_level_id = il.intensity_level_id
JOIN 
    {{ ref('dim_personal_activity_tag') }} pat
    ON hm.personal_activity_tag_id = pat.personal_activity_tag_id