{{ config(
    materialized='table',
    tags=['fact_minute_metrics', 'mart', 'fact', 'metrics'],
    description="This table contains minute-level metrics data from Fitbit, including calories burned, intensity, METs, and step counts."
) }}

WITH deduplicated AS (
    SELECT
        mm.user_id,
        c.minute_timestamp AS activity_minute,
        mm.avg_calories,
        mm.avg_intensity,
        mm.avg_mets,
        mm.avg_steps,
        cbl.calorie_level_id,
        scl.step_count_level_id,
        il.intensity_level_id,
        pat.personal_activity_tag_id,
        ROW_NUMBER() OVER (PARTITION BY mm.user_id, c.minute_timestamp ORDER BY mm.user_id) AS row_num
    FROM 
        {{ ref('int_minute_metrics') }} mm
    LEFT JOIN 
        {{ ref('dim_users') }} u 
        ON mm.user_id = u.user_id
    LEFT JOIN 
        {{ ref('dim_calendar') }} c 
        ON mm.activity_minute = c.minute_timestamp
    LEFT JOIN 
        {{ ref('dim_calorie_burn_level') }} cbl 
        ON mm.calorie_burner_level = cbl.calorie_burn_level
    LEFT JOIN 
        {{ ref('dim_step_count_level') }} scl 
        ON mm.step_count_level = scl.step_count_level
    LEFT JOIN 
        {{ ref('dim_intensity_level') }} il 
        ON mm.intensity_level = il.intensity_level
    LEFT JOIN 
        {{ ref('dim_personal_activity_tag') }} pat 
        ON mm.personal_activity_tag = pat.personal_activity_tag
)

SELECT
    user_id,
    activity_minute,
    avg_calories,
    avg_intensity,
    avg_mets,
    avg_steps,
    calorie_level_id,
    step_count_level_id,
    intensity_level_id,
    personal_activity_tag_id
FROM deduplicated
WHERE row_num = 1
