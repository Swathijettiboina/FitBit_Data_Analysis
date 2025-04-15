{{
  config(
    materialized = 'table',
    tags        = ['heart_rate_analysis', 'minute_metrics',"heart_core"],
    description = 'Combined minute-level activity metrics with heart rate data, inlcusing steps ,and calories burned.'
  )
}}

WITH heart_rate_aggregated AS (
    SELECT
        id                          AS user_id,
        DATE_TRUNC('minute', time)  AS activity_minute,
        AVG(value)                  AS avg_heart_rate,
        MIN(value)                  AS min_heart_rate,
        MAX(value)                  AS max_heart_rate,
        COUNT(*)                    AS heart_rate_readings
    FROM {{ ref('stg_heartrateseconds') }}
    GROUP BY user_id, activity_minute
)

SELECT
    -- Base identifiers
    h.user_id                                     AS user_id,
    COALESCE(s.activityminute, 
             c.activityminute, 
             h.activit_yminute)                    AS activity_minute,
    
    -- Activity metrics
    COALESCE(s.steps, 0)                          AS steps,
    COALESCE(c.calories, 0)                       AS calories,
    
    -- Heart rate metrics
    h.avg_heart_rate                              AS avg_heart_rate,
    h.min_heart_rate                              AS min_heart_rate,
    h.max_heart_rate                              AS max_heart_rate,
    h.heart_rate_readings                         AS heart_rate_readings,
    
    -- Derived metrics
    CASE
        WHEN h.avg_heart_rate BETWEEN 0 AND 59    THEN 'resting'
        WHEN h.avg_heart_rate BETWEEN 60 AND 99   THEN 'light'
        WHEN h.avg_heart_rate BETWEEN 100 AND 139 THEN 'moderate'
        WHEN h.avg_heart_rate >= 140              THEN 'intense'
        ELSE                                           NULL
    END                                           AS heart_rate_zone,
    
    CASE
        WHEN s.steps = 0                          THEN 'sedentary_steps'
        WHEN s.steps BETWEEN 1 AND 50             THEN 'light_activity_steps'
        WHEN s.steps BETWEEN 51 AND 100           THEN 'moderate_activity_steps'
        WHEN s.steps > 100                        THEN 'active_steps'
        ELSE                                           NULL
    END                                           AS step_intensity,
    
    ROUND(COALESCE(c.calories, 0) / 
          NULLIF(COALESCE(s.steps, 0), 0), 4)     AS calories_per_step

FROM {{ ref('stg_minutestepsnarrow') }} s
FULL OUTER JOIN {{ ref('stg_minutecalories') }} c
    ON s.user_id = c.user_id 
    AND s.activity_minute = c.activity_minute
FULL OUTER JOIN heart_rate_aggregated h
    ON COALESCE(s.user_id, c.user_id) = h.user_id 
    AND COALESCE(s.activity_minute, c.activity_minute) = h.activity_minute
ORDER BY user_id, activity_minute