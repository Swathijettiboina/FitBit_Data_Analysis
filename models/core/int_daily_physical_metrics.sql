{{
  config(
    materialized='table',
    tags=['daily_metrics','activity_sleep',"physical_metrics"],
    description="Daily activity and sleep metrics including steps, calories, and sleep quality"
  )
}}

WITH daily_calories AS (
    SELECT
        id AS user_id,
        activityday AS activity_date,
        calories AS daily_calories
    FROM {{ ref('stg_dailycalories') }}
),

daily_steps AS (
    SELECT
        id AS user_id,
        activityday AS activity_date,
        steptotal AS total_steps
    FROM {{ ref('stg_dailysteps') }}
),

sleep_data AS (
    SELECT
        id AS user_id,
        DATE(sleepday) AS activity_date,
        totalminutesasleep AS sleep_minutes,
        totaltimeinbed AS time_in_bed_minutes
    FROM {{ ref('stg_sleepday') }}
)

SELECT
    dc.user_id,
    dc.activity_date,
    
    -- Activity metrics
    ds.total_steps,
    dc.daily_calories AS calories_burned,
    
    -- Step efficiency
    ROUND(dc.daily_calories / NULLIF(ds.total_steps, 0), 4) AS calories_per_step,
    
    -- Activity classification
    CASE
        WHEN ds.total_steps >= 10000 THEN 'Highly Active'
        WHEN ds.total_steps >= 7500 THEN 'Moderately Active'
        WHEN ds.total_steps >= 5000 THEN 'Lightly Active'
        ELSE 'Sedentary'
    END AS activity_level,
    
    -- Sleep metrics
    sl.sleep_minutes,
    sl.time_in_bed_minutes,
    sl.time_in_bed_minutes - sl.sleep_minutes AS awake_in_bed_minutes,
    ROUND(sl.sleep_minutes * 100.0 / NULLIF(sl.time_in_bed_minutes, 0), 1) AS sleep_efficiency_percent,
    
    -- Sleep quality rating
    CASE
        WHEN sl.sleep_minutes >= 480 THEN 'Excellent'
        WHEN sl.sleep_minutes >= 360 THEN 'Good'
        WHEN sl.sleep_minutes >= 240 THEN 'Fair'
        WHEN sl.sleep_minutes > 0 THEN 'Poor'
        ELSE NULL
    END AS sleep_quality
    
FROM daily_calories dc
LEFT JOIN daily_steps ds 
    ON dc.user_id = ds.user_id 
    AND dc.activity_date = ds.activity_date
LEFT JOIN sleep_data sl 
    ON dc.user_id = sl.user_id 
    AND dc.activity_date = sl.activity_date