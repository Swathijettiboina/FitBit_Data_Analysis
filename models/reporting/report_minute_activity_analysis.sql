{{
    config(
        materialized='table',
        tags=['report', 'minute', 'activity','analysis'],
        description="This table contains minute-level activity analysis data from Fitbit devices, including heart rate, calories burned, and step counts."
    )
}}

SELECT
    mm.user_id,
    mm.activity_minute,
    mm.avg_calories,
    mm.avg_intensity,
    mm.avg_mets,
    mm.avg_steps,
    cbl.calorie_burn_level,
    scl.step_count_level,
    il.intensity_level,
    pat.personal_activity_tag,
    mhr.avg_heart_rate,
    mhr.min_heart_rate,
    mhr.max_heart_rate,
    mhr.heart_rate_readings,
    hrz.heart_rate_zone,
    mhr.calories_burnerd_per_step
FROM
    {{ ref('fact_minute_metrics') }} mm
JOIN
    {{ ref('fact_minute_heart_rate_analysis') }} mhr
    ON mm.user_id = mhr.user_id AND mm.activity_minute = mhr.activity_minute
JOIN
    {{ ref('dim_heart_rate_zone') }} hrz
    ON mhr.heart_rate_zone_id = hrz.heart_rate_zone_id
JOIN
    {{ ref('dim_calorie_burn_level') }} cbl
    ON mm.calorie_level_id=cbl.calorie_level_id
JOIN
    {{ ref('dim_step_count_level') }} scl
    ON mm.step_count_level_id=scl.step_count_level_id
JOIN
    {{ ref('dim_intensity_level') }} il
    ON mm.intensity_level_id=il.intensity_level_id
JOIN
    {{ ref('dim_personal_activity_tag') }} pat  
    ON mm.personal_activity_tag_id=pat.personal_activity_tag_id