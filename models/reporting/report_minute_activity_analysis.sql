{{ 
    config(
        materialized='table',
        tags=['report', 'rp_minute', 'activity','analysis'],
        description="This table contains minute-level activity analysis data from Fitbit devices, including heart rate, calories burned, and step counts."
    )
}}

WITH unique_minute_data AS (
    SELECT DISTINCT
        mm.user_id,
        mm.activity_minute,
        mm.avg_calories,
        mm.avg_intensity,
        mm.avg_mets,
        mm.avg_steps,
        mm.calorie_level_id,
        mm.step_count_level_id,
        mm.intensity_level_id,
        mm.personal_activity_tag_id
    FROM {{ ref('fact_minute_metrics') }} mm
)

SELECT
    umd.user_id,
    umd.activity_minute,
    umd.avg_calories,
    umd.avg_intensity,
    umd.avg_mets,
    umd.avg_steps,
    COALESCE(cbl.calorie_burn_level, 'No record') AS calorie_burn_level,
    COALESCE(scl.step_count_level, 'No record') AS step_count_level,
    COALESCE(il.intensity_level, 'No record') AS intensity_level,
    COALESCE(pat.personal_activity_tag, 'No record') AS personal_activity_tag,
    COALESCE(mhr.avg_heart_rate, 0) AS avg_heart_rate,
    COALESCE(mhr.min_heart_rate, 0) AS min_heart_rate,
    COALESCE(mhr.max_heart_rate, 0) AS max_heart_rate,
    COALESCE(mhr.heart_rate_readings, 0) AS heart_rate_readings,
    COALESCE(hrz.heart_rate_zone, 'No record') AS heart_rate_zone,
    COALESCE(mhr.calories_burnerd_per_step, 0) AS calories_burned_per_step
FROM unique_minute_data umd
LEFT JOIN {{ ref('fact_minute_heart_rate_analysis') }} mhr
    ON umd.user_id = mhr.user_id AND umd.activity_minute = mhr.activity_minute
LEFT JOIN {{ ref('dim_heart_rate_zone') }} hrz
    ON mhr.heart_rate_zone_id = hrz.heart_rate_zone_id
LEFT JOIN {{ ref('dim_calorie_burn_level') }} cbl
    ON umd.calorie_level_id = cbl.calorie_level_id
LEFT JOIN {{ ref('dim_step_count_level') }} scl
    ON umd.step_count_level_id = scl.step_count_level_id
LEFT JOIN {{ ref('dim_intensity_level') }} il
    ON umd.intensity_level_id = il.intensity_level_id
LEFT JOIN {{ ref('dim_personal_activity_tag') }} pat
    ON umd.personal_activity_tag_id = pat.personal_activity_tag_id
