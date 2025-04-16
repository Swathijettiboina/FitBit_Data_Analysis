{{ 
  config(
    materialized = 'table',
    tags        = ['heart_rate_analysis', 'minute_metrics', 'heart_core'],
    description = 'Aggregated heart rate data at second level and grouped by minute, including steps and calories burned.'
  ) 
}}
WITH heart_rate_aggregated AS (
    {{ aggregate_seconds_to_minutes_with_column(
        table_name = 'stg_heartrateseconds', 
        column_name = 'value', 
        timestamp_column = 'time',  
        renamed_column_name = 'heart_rate'
    ) }}
)

SELECT
    h.user_id,
    h.activity_minute,  

    COALESCE(s.steps, 0)                          AS steps,
    COALESCE(c.calories, 0)                       AS calories,

    h.avg_heart_rate,
    h.min_heart_rate,
    h.max_heart_rate,
    h.heart_rate_readings,

    -- Derived metrics
    CASE
        WHEN h.avg_heart_rate BETWEEN 0 AND 59    THEN 'Resting'
        WHEN h.avg_heart_rate BETWEEN 60 AND 79   THEN 'Warm-up'
        WHEN h.avg_heart_rate BETWEEN 80 AND 99   THEN 'Fat Burn'
        WHEN h.avg_heart_rate BETWEEN 100 AND 119 THEN 'Cardio'
        WHEN h.avg_heart_rate BETWEEN 120 AND 139 THEN 'Peak'
        WHEN h.avg_heart_rate >= 140              THEN 'Max'
        ELSE 'Unknown'

    END AS heart_rate_zone,

    ROUND(COALESCE(c.calories, 0) / 
          NULLIF(COALESCE(s.steps, 0), 0), 4)     AS calories_burnerd_per_step

FROM heart_rate_aggregated h
LEFT JOIN {{ ref('stg_minutestepsnarrow') }} s
    ON h.user_id = s.id 
    AND h.activity_minute = s.activityminute  
LEFT JOIN {{ ref('stg_minutecalories') }} c
    ON h.user_id = c.id 
    AND h.activity_minute = c.activityminute 

ORDER BY h.user_id, h.activity_minute
