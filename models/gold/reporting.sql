-- models/kpi_consolidated_report_with_hour_minute.sql
{{ config(
    materialized='table',
    tags=['kpi', 'consolidated', 'hourly', 'minute'],
    description='Consolidated table for all Fitbit KPIs with drilldowns for hours and minutes.'
) }}

WITH total_hourly_steps AS (
    SELECT
        user_id,
        activity_hour,
        total_steps
    FROM {{ ref('kpi_total_hourly_steps') }}
),
total_minute_steps AS (
    SELECT
        user_id,
        activity_minute,
        total_steps
    FROM {{ ref('kpi_total_minute_steps') }}
),
total_hourly_calories AS (
    SELECT
        user_id,
        activity_hour,
        total_calories
    FROM {{ ref('kpi_total_hourly_calories') }}
),
total_minute_calories AS (
    SELECT
        user_id,
        activity_minute,
        total_calories
    FROM {{ ref('kpi_total_minute_calories') }}
),
average_hourly_heart_rate AS (
    SELECT
        user_id,
        activity_hour,
        average_heart_rate
    FROM {{ ref('kpi_average_hourly_heart_rate') }}
),
average_minute_heart_rate AS (
    SELECT
        user_id,
        activity_minute,
        average_heart_rate
    FROM {{ ref('kpi_average_minute_heart_rate') }}
),
active_minutes_hour AS (
    SELECT
        user_id,
        activity_hour,
        total_active_minutes
    FROM {{ ref('kpi_active_minutes_hour') }}
),
active_minutes_minute AS (
    SELECT
        user_id,
        activity_minute,
        total_active_minutes
    FROM {{ ref('kpi_active_minutes_minute') }}
),
sedentary_minutes_hour AS (
    SELECT
        user_id,
        activity_hour,
        total_sedentary_minutes
    FROM {{ ref('kpi_sedentary_minutes_hour') }}
),
sedentary_minutes_minute AS (
    SELECT
        user_id,
        activity_minute,
        total_sedentary_minutes
    FROM {{ ref('kpi_sedentary_minutes_minute') }}
),
sleep_duration_hour AS (
    SELECT
        user_id,
        activity_hour,
        total_sleep_duration
    FROM {{ ref('kpi_sleep_duration_hour') }}
),
sleep_duration_minute AS (
    SELECT
        user_id,
        activity_minute,
        total_sleep_duration
    FROM {{ ref('kpi_sleep_duration_minute') }}
),
total_hourly_distance AS (
    SELECT
        user_id,
        activity_hour,
        total_distance
    FROM {{ ref('kpi_total_hourly_distance') }}
),
total_minute_distance AS (
    SELECT
        user_id,
        activity_minute,
        total_distance
    FROM {{ ref('kpi_total_minute_distance') }}
)

SELECT
    s.user_id,
    s.activity_date,
    s.activity_hour,
    s.activity_minute,
    s.total_steps AS daily_steps,
    t.total_steps AS hourly_steps,
    m.total_steps AS minute_steps,
    c.total_calories AS daily_calories,
    th.total_calories AS hourly_calories,
    tm.total_calories AS minute_calories,
    hr.average_heart_rate AS daily_heart_rate,
    hhr.average_heart_rate AS hourly_heart_rate,
    mhr.average_heart_rate AS minute_heart_rate,
    am.total_active_minutes AS daily_active_minutes,
    ah.total_active_minutes AS hourly_active_minutes,
    amn.total_active_minutes AS minute_active_minutes,
    sm.total_sedentary_minutes AS daily_sedentary_minutes,
    sh.total_sedentary_minutes AS hourly_sedentary_minutes,
    smn.total_sedentary_minutes AS minute_sedentary_minutes,
    sd.total_sleep_duration AS daily_sleep_duration,
    sdhr.total_sleep_duration AS hourly_sleep_duration,
    sdmin.total_sleep_duration AS minute_sleep_duration,
    d.total_distance AS daily_distance,
    dh.total_distance AS hourly_distance,
    dm.total_distance AS minute_distance
FROM total_daily_steps s
LEFT JOIN total_hourly_steps t ON s.user_id = t.user_id AND s.activity_hour = t.activity_hour
LEFT JOIN total_minute_steps m ON s.user_id = m.user_id AND s.activity_minute = m.activity_minute
LEFT JOIN total_daily_calories c ON s.user_id = c.user_id AND s.activity_date = c.activity_date
LEFT JOIN total_hourly_calories th ON s.user_id = th.user_id AND s.activity_hour = th.activity_hour
LEFT JOIN total_minute_calories tm ON s.user_id = tm.user_id AND s.activity_minute = tm.activity_minute
LEFT JOIN average_heart_rate hr ON s.user_id = hr.user_id AND s.activity_date = hr.activity_date
LEFT JOIN average_hourly_heart_rate hhr ON s.user_id = hhr.user_id AND s.activity_hour = hhr.activity_hour
LEFT JOIN average_minute_heart_rate mhr ON s.user_id = mhr.user_id AND s.activity_minute = mhr.activity_minute
LEFT JOIN active_minutes am ON s.user_id = am.user_id AND s.activity_date = am.activity_date
LEFT JOIN active_minutes_hour ah ON s.user_id = ah.user_id AND s.activity_hour = ah.activity_hour
LEFT JOIN active_minutes_minute amn ON s.user_id = amn.user_id AND s.activity_minute = amn.activity_minute
LEFT JOIN sedentary_minutes sm ON s.user_id = sm.user_id AND s.activity_date = sm.activity_date
LEFT JOIN sedentary_minutes_hour sh ON s.user_id = sh.user_id AND s.activity_hour = sh.activity_hour
LEFT JOIN sedentary_minutes_minute smn ON s.user_id = smn.user_id AND s.activity_minute = smn.activity_minute
LEFT JOIN sleep_duration sd ON s.user_id = sd.user_id AND s.activity_date = sd.activity_date
LEFT JOIN sleep_duration_hour sdhr ON s.user_id = sdhr.user_id AND s.activity_hour = sdhr.activity_hour
LEFT JOIN sleep_duration_minute sdmin ON s.user_id = sdmin.user_id AND s.activity_minute = sdmin.activity_minute
LEFT JOIN total_daily_distance d ON s.user_id = d.user_id AND s.activity_date = d.activity_date
LEFT JOIN total_hourly_distance dh ON s.user_id = dh.user_id AND s.activity_hour = dh.activity_hour
LEFT JOIN total_minute_distance dm ON s.user_id = dm.user_id AND s.activity_minute = dm.activity_minute
