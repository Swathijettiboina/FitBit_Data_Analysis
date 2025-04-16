{{ 
    config(
        materialized='table',
        unique_key='id',
        tags=['report', 'hourly', 'activity', 'rpt_hourly_analysis'],
        description="This table contains hourly activity analysis data from Fitbit devices."
    )
}}

WITH unique_hourly_data AS (
    SELECT DISTINCT
        hm.user_id,
        hm.activity_hour,
        hm.avg_calories,
        hm.avg_intensity,
        hm.avg_steps,
        hm.calorie_level_id,
        hm.step_count_level_id,
        hm.intensity_level_id,
        hm.personal_activity_tag_id
    FROM {{ ref('fact_hourly_metrics') }} hm
)

SELECT
    uhd.user_id,
    uhd.activity_hour,
    uhd.avg_calories,
    uhd.avg_intensity,
    uhd.avg_steps,
    COALESCE(cbl.calorie_burn_level, 'No record') AS calorie_burn_level,
    COALESCE(scl.step_count_level, 'No record') AS step_count_level,
    COALESCE(il.intensity_level, 'No record') AS intensity_level,
    COALESCE(pat.personal_activity_tag, 'No record') AS personal_activity_tag
FROM unique_hourly_data uhd
LEFT JOIN {{ ref('dim_calorie_burn_level') }} cbl
    ON uhd.calorie_level_id = cbl.calorie_level_id
LEFT JOIN {{ ref('dim_step_count_level') }} scl
    ON uhd.step_count_level_id = scl.step_count_level_id
LEFT JOIN {{ ref('dim_intensity_level') }} il
    ON uhd.intensity_level_id = il.intensity_level_id
LEFT JOIN {{ ref('dim_personal_activity_tag') }} pat
    ON uhd.personal_activity_tag_id = pat.personal_activity_tag_id
