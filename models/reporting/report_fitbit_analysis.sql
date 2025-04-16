{{
    config(
        materialized='table',
        tags=['reporting', 'full_activity_report'],
        description="Unified activity report combining minute, hourly, and daily data with granularity column for Power BI slicing."
    )
}}

WITH minute_data AS (
    SELECT
        mm.user_id,
        mm.activity_minute                      AS activity_timestamp,
        'minute'                                AS granularity,
        mm.avg_calories,
        mm.avg_intensity,
        mm.avg_mets,
        mm.avg_steps,
        mm.calorie_level_id,
        mm.step_count_level_id,
        mm.intensity_level_id,
        mm.personal_activity_tag_id,
        mhr.avg_heart_rate,
        mhr.min_heart_rate,
        mhr.max_heart_rate,
        mhr.heart_rate_readings,
        hrz.heart_rate_zone,
        mhr.calories_burnerd_per_step
    FROM 
        {{ ref('fact_minute_metrics') }} mm
    LEFT JOIN 
        {{ ref('fact_minute_heart_rate_analysis') }} mhr
        ON mm.user_id = mhr.user_id AND mm.activity_minute = mhr.activity_minute
    LEFT JOIN 
        {{ ref('dim_heart_rate_zone') }} hrz
        ON mhr.heart_rate_zone_id = hrz.heart_rate_zone_id
),

hourly_data AS (
    SELECT
        hm.user_id,
        hm.activity_hour                        AS activity_timestamp,
        'hour'                                  AS granularity,
        hm.avg_calories,
        hm.avg_intensity,
        NULL                                    AS avg_mets,
        hm.avg_steps,
        hm.calorie_level_id,
        hm.step_count_level_id,
        hm.intensity_level_id,
        hm.personal_activity_tag_id,
        NULL                                    AS avg_heart_rate,
        NULL                                    AS min_heart_rate,
        NULL                                    AS max_heart_rate,
        NULL                                    AS heart_rate_readings,
        NULL                                    AS heart_rate_zone,
        NULL                                    AS calories_burnerd_per_step
    FROM {{ ref('fact_hourly_metrics') }} hm
),

daily_data AS (
    SELECT
        da.user_id,
        da.activity_date                        AS activity_timestamp,
        'day'                                   AS granularity,
        NULL                                    AS avg_calories,
        NULL                                    AS avg_intensity,
        NULL                                    AS avg_mets,
        NULL                                    AS avg_steps,
        da.calorie_level_id,
        da.step_count_level_id,
        NULL                                    AS intensity_level_id,
        pm.personal_activity_tag_id,
        NULL                                    AS avg_heart_rate,
        NULL                                    AS min_heart_rate,
        NULL                                    AS max_heart_rate,
        NULL                                    AS heart_rate_readings,
        NULL                                    AS heart_rate_zone,
        NULL                                    AS calories_burnerd_per_step
    FROM {{ ref('fact_daily_activity') }} da
    LEFT JOIN {{ ref('fact_daily_physical_metrics') }} pm
        ON da.user_id = pm.user_id AND da.activity_date = pm.activity_date
)

-- UNION ALL
SELECT
    user_id,
    activity_timestamp,
    granularity,
    avg_calories,
    avg_intensity,
    avg_mets,
    avg_steps,
    COALESCE(scl.step_count_level, 'No record')         AS step_count_level,
    COALESCE(cbl.calorie_burn_level, 'No record')       AS calorie_burn_level,
    COALESCE(il.intensity_level, 'No record')           AS intensity_level,
    COALESCE(pat.personal_activity_tag, 'No record')    AS personal_activity_tag,
    avg_heart_rate,
    min_heart_rate,
    max_heart_rate,
    heart_rate_readings,
    heart_rate_zone,
    calories_burnerd_per_step
FROM (
    SELECT * FROM minute_data
    UNION ALL
    SELECT * FROM hourly_data
    UNION ALL
    SELECT * FROM daily_data
) combined
LEFT JOIN 
    {{ ref('dim_step_count_level') }} scl
    ON combined.step_count_level_id = scl.step_count_level_id

LEFT JOIN 
    {{ ref('dim_calorie_burn_level') }} cbl
    ON combined.calorie_level_id = cbl.calorie_level_id

LEFT JOIN 
    {{ ref('dim_intensity_level') }} il
    ON combined.intensity_level_id = il.intensity_level_id

LEFT JOIN 
    {{ ref('dim_personal_activity_tag') }} pat
    ON combined.personal_activity_tag_id = pat.personal_activity_tag_id
