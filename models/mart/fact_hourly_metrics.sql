{{ config(
    materialized='table',
    tags=['fact_hourly_metrics', 'fact', 'mart', 'metrics'],
    description="Fact table containing hourly aggregated fitness metrics including calories burned, activity intensity, and step counts. Joined with dimension tables for classification levels."
) }}

WITH deduplicated AS (
    SELECT
        hm.user_id,
        hm.activity_hour,
        hm.avg_calories,
        hm.avg_intensity,
        hm.avg_steps,
        cbl.calorie_level_id,
        scl.step_count_level_id,
        il.intensity_level_id,
        pat.personal_activity_tag_id,
        ROW_NUMBER() OVER (PARTITION BY hm.user_id, hm.activity_hour ORDER BY hm.user_id) AS row_num
    FROM 
        {{ ref('int_hourly_metrics') }} hm
    LEFT JOIN 
        {{ ref('dim_calorie_burn_level') }} cbl 
        ON hm.calorie_burner_level = cbl.calorie_burn_level
    LEFT JOIN 
        {{ ref('dim_step_count_level') }} scl 
        ON hm.step_count_level = scl.step_count_level
    LEFT JOIN 
        {{ ref('dim_intensity_level') }} il 
        ON hm.intensity_level = il.intensity_level
    LEFT JOIN 
        {{ ref('dim_personal_activity_tag') }} pat 
        ON hm.personal_activity_tag = pat.personal_activity_tag
)

SELECT
    user_id,
    activity_hour,
    avg_calories,
    avg_intensity,
    avg_steps,
    calorie_level_id,
    step_count_level_id,
    intensity_level_id,
    personal_activity_tag_id
FROM deduplicated
WHERE row_num = 1