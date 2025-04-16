{{
    config(
        materialized='table',
        tags=['int', 'minute','mart','fact_heart_rate', 'analysis'],
        description="This table contains minute-level heart rate analysis data from Fitbit devices."
    )
}}

SELECT
    u.user_id,
    c.minute_timestamp AS activity_minute,
    hra.steps,
    hra.calories,
    hra.avg_heart_rate,
    hra.min_heart_rate,
    hra.max_heart_rate,
    hra.heart_rate_readings,
    hrz.heart_rate_zone_id,
    hra.calories_burnerd_per_step
FROM 
    {{ ref('int_minute_heart_rate_analysis') }} hra
JOIN 
    {{ ref('dim_users') }} u 
    ON hra.user_id = u.user_id
JOIN 
    {{ ref('dim_calendar') }} c
    ON hra.activity_minute = c.minute_timestamp
JOIN 
    {{ ref('dim_heart_rate_zone') }} hrz
    ON hra.heart_rate_zone = hrz.heart_rate_zone