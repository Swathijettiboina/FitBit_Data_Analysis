{{
    config(
        materialized = 'table',
        tags         = ['core', 'minute_metrics', 'physical'],
        description  = 'Core model with user-level minute-level physical activity metrics including calories, intensity, METs, and steps.'
    )
}}

WITH aggregated_calories AS (
    {{ aggregate_table_by_minute('stg_minutecalories', 'CALORIES', time_granularity='minute', renamed_column_name='calories') }}
),

aggregated_intensity AS (
    {{ aggregate_table_by_minute('stg_minuteintensitiesnarrow', 'INTENSITY', time_granularity='minute', renamed_column_name='intensity') }}
),

aggregated_mets AS (
    {{ aggregate_table_by_minute('stg_minutemetsnarrow', 'METS', time_granularity='minute', renamed_column_name='mets') }}
),

aggregated_steps AS (
    {{ aggregate_table_by_minute('stg_minutestepsnarrow', 'STEPS', time_granularity='minute', renamed_column_name='steps') }}
)

SELECT
    cal.user_id,
    cal.activity_minute,

    -- Metrics
    cal.avg_calories,
    int.avg_intensity,
    mts.avg_mets,
    stp.avg_steps,

    -- Calorie Burn Tag
    CASE
        WHEN cal.avg_calories > 5 THEN 'High Calorie Burner'
        WHEN cal.avg_calories BETWEEN 3 AND 5 THEN 'Moderate Calorie Burner'
        WHEN cal.avg_calories BETWEEN 1 AND 3 THEN 'Low Calorie Burner'
        ELSE 'Sedentary'
    END AS calorie_burn_tag,

    -- Intensity Tag
    CASE
        WHEN int.avg_intensity > 8 THEN 'High Intensity'
        WHEN int.avg_intensity BETWEEN 5 AND 8 THEN 'Moderate Intensity'
        WHEN int.avg_intensity BETWEEN 3 AND 5 THEN 'Low Intensity'
        ELSE 'Very Low Intensity'
    END AS intensity_tag,

    -- Steps Tag
    CASE
        WHEN stp.avg_steps > 20 THEN 'Highly Active'
        WHEN stp.avg_steps BETWEEN 10 AND 20 THEN 'Moderately Active'
        WHEN stp.avg_steps BETWEEN 1 AND 10 THEN 'Lightly Active'
        ELSE 'No Activity'
    END AS step_activity_tag,

    -- Combined Personal Activity Tag
    CASE
        WHEN cal.avg_calories > 5 AND stp.avg_steps > 20 AND int.avg_intensity > 8 THEN 'Extremely Active'
        WHEN cal.avg_calories BETWEEN 3 AND 5 AND stp.avg_steps BETWEEN 10 AND 20 AND int.avg_intensity BETWEEN 5 AND 8 THEN 'Moderately Active'
        WHEN cal.avg_calories BETWEEN 1 AND 3 AND stp.avg_steps BETWEEN 1 AND 10 AND int.avg_intensity BETWEEN 3 AND 5 THEN 'Lightly Active'
        ELSE 'Inactive'
    END AS personal_activity_tag

FROM aggregated_calories cal
LEFT JOIN aggregated_intensity int 
    ON cal.user_id = int.user_id AND cal.activity_minute = int.activity_minute
LEFT JOIN aggregated_mets mts 
    ON cal.user_id = mts.user_id AND cal.activity_minute = mts.activity_minute
LEFT JOIN aggregated_steps stp 
    ON cal.user_id = stp.user_id AND cal.activity_minute = stp.activity_minute

ORDER BY cal.user_id, cal.activity_minute
