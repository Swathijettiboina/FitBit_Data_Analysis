{{ 
    config(
        MATERIALIZED = 'table',
        tags         = ['core', 'hourly_metrics', 'physical'],
        description  = 'Core model with user-level hourly physical activity metrics including steps, calories, and intensity breakdown. calories is the base table'
    ) 
}}

WITH 
-- Aggregating Calories data
aggregated_calories AS (
    {{ aggregate_single_table('stg_hourlycalories', 'CALORIES', time_granularity='hour', renamed_column_name='calories') }}
),

-- Aggregating Intensity data
aggregated_intensity AS (
    {{ aggregate_single_table('stg_hourlyintensities', 'TOTALINTENSITY', time_granularity='hour', renamed_column_name='intensity') }}
),

-- Aggregating Steps data
aggregated_steps AS (
    {{ aggregate_single_table('stg_hourlysteps', 'STEPTOTAL', time_granularity='hour', renamed_column_name='steps') }}
)

SELECT
    cal.user_id,
    cal.activity_hour,
    cal.avg_calories,
    int.avg_intensity,
    stp.avg_steps,

    -- Clear Tags for Calories
    CASE
        WHEN cal.avg_calories > 500 THEN 'High Calorie Burner'
        WHEN cal.avg_calories BETWEEN 300 AND 500 THEN 'Moderate Calorie Burner'
        WHEN cal.avg_calories BETWEEN 100 AND 300 THEN 'Low Calorie Burner'
        ELSE 'Sedentary'
    END AS calorie_burn_tag,

    -- Clear Tags for Steps
    CASE
        WHEN stp.avg_steps > 12000 THEN 'Highly Active'
        WHEN stp.avg_steps BETWEEN 8000 AND 12000 THEN 'Moderately Active'
        WHEN stp.avg_steps BETWEEN 4000 AND 8000 THEN 'Lightly Active'
        WHEN stp.avg_steps > 0 THEN 'Sedentary'
        ELSE 'No Activity'
    END AS step_activity_tag,

    -- Clear Tags for Intensity
    CASE
        WHEN int.avg_intensity > 80 THEN 'High Intensity'
        WHEN int.avg_intensity BETWEEN 50 AND 80 THEN 'Moderate Intensity'
        WHEN int.avg_intensity BETWEEN 30 AND 50 THEN 'Low Intensity'
        ELSE 'Very Low Intensity'
    END AS intensity_tag,

    CASE
        WHEN cal.avg_calories > 500 AND stp.avg_steps > 12000 AND int.avg_intensity > 80 THEN 'Extremely Active'
        WHEN cal.avg_calories BETWEEN 300 AND 500 AND stp.avg_steps BETWEEN 8000 AND 12000 AND int.avg_intensity BETWEEN 50 AND 80 THEN 'Moderately Active'
        WHEN cal.avg_calories BETWEEN 100 AND 300 AND stp.avg_steps BETWEEN 4000 AND 8000 AND int.avg_intensity BETWEEN 30 AND 50 THEN 'Lightly Active'
        ELSE 'Inactive'
    END AS personal_activity_tag

FROM aggregated_calories cal
LEFT JOIN aggregated_intensity int ON cal.user_id = int.user_id AND cal.activity_hour = int.activity_hour
LEFT JOIN aggregated_steps stp ON cal.user_id = stp.user_id AND cal.activity_hour = stp.activity_hour

ORDER BY cal.user_id, cal.activity_hour