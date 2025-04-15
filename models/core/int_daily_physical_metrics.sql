{{ 
  config(
    materialized = 'table',
    tags         = ['core', 'daily_metrics', 'physical'],
    description  = 'Daily-level core model with user-level physical activity metrics including steps, calories, and intensity breakdown.'
  ) 
}}

WITH daily_activity AS (
    SELECT
        id                 AS user_id,
        activitydate       AS activity_date,
        sedentaryminutes   AS sedentary_minutes,
        totalsteps         AS steps_total,
        calories           AS calories_total
    FROM {{ ref('stg_dailyactivity') }}
),

daily_calories AS (
    SELECT
        id            AS user_id,
        activityday   AS activity_date,
        calories      AS calories_daily
    FROM {{ ref('stg_dailycalories') }}
),

daily_steps AS (
    SELECT
        id           AS user_id,
        activityday  AS activity_date,
        steptotal    AS steps_daily
    FROM {{ ref('stg_dailysteps') }}
),

daily_intensities AS (
    SELECT
        id                    AS user_id,
        activityday           AS activity_date,
        lightlyactiveminutes  AS minutes_light,
        fairlyactiveminutes   AS minutes_fair,
        veryactiveminutes     AS minutes_very
    FROM {{ ref('stg_dailyintensities') }}
)

SELECT
    da.user_id                                          AS user_id,
    da.activity_date                                    AS activity_date,

    COALESCE(ds.steps_daily, da.steps_total)            AS total_steps,
    COALESCE(dc.calories_daily, da.calories_total)      AS total_calories,

    di.minutes_light                                     AS lightly_active_minutes,
    di.minutes_fair                                      AS fairly_active_minutes,
    di.minutes_very                                      AS very_active_minutes,
    da.sedentary_minutes                                 AS sedentary_minutes,

    (di.minutes_light + di.minutes_fair + di.minutes_very) AS active_minutes_total,

    ROUND(
        COALESCE(dc.calories_daily, da.calories_total) 
        / NULLIF(COALESCE(ds.steps_daily, da.steps_total), 0), 2
    )                                                  AS calories_per_step,

    CASE
        WHEN COALESCE(ds.steps_daily, da.steps_total) >= 10000 THEN 'highly_active'
        WHEN COALESCE(ds.steps_daily, da.steps_total) >= 7500  THEN 'moderately_active'
        WHEN COALESCE(ds.steps_daily, da.steps_total) >= 5000  THEN 'lightly_active'
        ELSE                                                       'sedentary'
    END                                               AS activity_level

FROM daily_activity da
LEFT JOIN daily_calories dc 
       ON da.user_id = dc.user_id 
      AND da.activity_date = dc.activity_date
LEFT JOIN daily_steps ds 
       ON da.user_id = ds.user_id 
      AND da.activity_date = ds.activity_date
LEFT JOIN daily_intensities di 
       ON da.user_id = di.user_id 
      AND da.activity_date = di.activity_date
