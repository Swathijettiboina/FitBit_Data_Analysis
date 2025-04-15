{{ config(materialized='table') }}

WITH base_dates AS (
    SELECT 
        u.user_id,
        d.date_key,
        d.date AS activity_date
    FROM {{ ref('dim_users') }} u
    CROSS JOIN {{ ref('dim_calendar') }} d
),

-- Daily level facts
daily_activity AS (
    SELECT 
        user_id,
        activity_date,
        total_steps,
        calories_burned,
        total_active_minutes
    FROM {{ ref('fact_daily_activity') }}
),

daily_sleep AS (
    SELECT 
        user_id,
        activity_date,
        total_minutes_asleep,
        total_time_in_bed,
        sleep_efficiency
    FROM {{ ref('fact_daily_sleep') }}
),

-- Hourly level facts
hourly_metrics AS (
    SELECT 
        user_id,
        DATE(activity_hour) AS activity_date,
        activity_hour,
        avg_calories,
        avg_intensity,
        avg_steps
    FROM {{ ref('fact_hourly_metrics') }}
),

-- Minute level metrics
minute_metrics AS (
    SELECT 
        user_id,
        DATE(activity_minute) AS activity_date,
        activity_minute,
        avg_calories AS minute_calories,
        avg_intensity AS minute_intensity,
        avg_steps AS minute_steps
    FROM {{ ref('fact_minute_metrics') }}
),

-- Minute heart rate
minute_heart_rate AS (
    SELECT 
        user_id,
        DATE(activity_minute) AS activity_date,
        activity_minute,
        avg_heart_rate,
        min_heart_rate,
        max_heart_rate
    FROM {{ ref('fact_minute_heart_rate') }}
)

-- Final reporting
SELECT 
    bd.user_id,
    bd.date_key,
    bd.activity_date,

    -- Daily
    COALESCE(da.total_steps, 0) AS total_steps,
    COALESCE(da.calories_burned, 0) AS calories_burned,
    COALESCE(da.total_active_minutes, 0) AS total_active_minutes,
    COALESCE(ds.total_minutes_asleep, 0) AS total_minutes_asleep,
    COALESCE(ds.sleep_efficiency, 0) AS sleep_efficiency,

    -- Hourly (aggregated to day level)
    ROUND(SUM(hm.avg_calories), 2) AS sum_hourly_avg_calories,
    ROUND(SUM(hm.avg_intensity), 2) AS sum_hourly_avg_intensity,
    ROUND(SUM(hm.avg_steps), 2) AS sum_hourly_avg_steps,

    -- Minute metrics (aggregated to day level)
    ROUND(SUM(mm.minute_calories), 2) AS sum_minute_calories,
    ROUND(SUM(mm.minute_intensity), 2) AS sum_minute_intensity,
    ROUND(SUM(mm.minute_steps), 2) AS sum_minute_steps,

    -- Heart rate stats (aggregated)
    ROUND(AVG(hr.avg_heart_rate), 2) AS avg_daily_heart_rate,
    MIN(hr.min_heart_rate) AS min_heart_rate_day,
    MAX(hr.max_heart_rate) AS max_heart_rate_day

FROM base_dates bd
LEFT JOIN daily_activity da ON bd.user_id = da.user_id AND bd.activity_date = da.activity_date
LEFT JOIN daily_sleep ds ON bd.user_id = ds.user_id AND bd.activity_date = ds.activity_date
LEFT JOIN hourly_metrics hm ON bd.user_id = hm.user_id AND bd.activity_date = hm.activity_date
LEFT JOIN minute_metrics mm ON bd.user_id = mm.user_id AND bd.activity_date = mm.activity_date
LEFT JOIN minute_heart_rate hr ON bd.user_id = hr.user_id AND bd.activity_date = hr.activity_date

GROUP BY 
    bd.user_id, bd.date_key, bd.activity_date,
    da.total_steps, da.calories_burned, da.total_active_minutes,
    ds.total_minutes_asleep, ds.sleep_efficiency
