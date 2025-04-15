{{ config(
    materialized = 'table',
    tags         = ['mart', 'fact_dailyactivity'],
    description  = 'Daily activity data from Fitbit merged with user data and combining the two files into one.'
) }}

-- Extract unique user IDs from all base activity tables
WITH cte_all_users AS (
    SELECT 
        DISTINCT user_id
    FROM {{ref('dim_users')}}
),

-- Get all available calendar dates
cte_all_dates AS (
    SELECT DISTINCT 
        date AS activity_date 
    FROM {{ ref('dim_calendar') }}
),

-- Create all combinations of users and dates
cte_all_user_dates AS (
    SELECT 
        u.user_id, 
        d.activity_date
    FROM cte_all_users u
    CROSS JOIN cte_all_dates d
),

-- Daily activity metrics
cte_activity AS (
    SELECT 
        id                    AS user_id,  
        activitydate          AS activity_date,  
        totalsteps            AS total_steps, 
        totaldistance         AS total_distance,
        veryactiveminutes     AS very_active_minutes,
        fairlyactiveminutes   AS fairly_active_minutes,
        lightlyactiveminutes  AS lightly_active_minutes,
        calories              AS total_calories
    FROM {{ ref('stg_dailyactivity') }}
),

-- Daily calories from another source
cte_calories AS (
    SELECT 
        id            AS user_id,  
        activityday   AS activity_date, 
        calories      AS daily_calories
    FROM {{ ref('stg_dailycalories') }}
),

-- Daily intensities (active minutes)
cte_intensities AS (
    SELECT 
        id                    AS user_id,  
        activityday           AS activity_date, 
        veryactiveminutes     AS very_active_minutes, 
        fairlyactiveminutes   AS fairly_active_minutes, 
        lightlyactiveminutes  AS lightly_active_minutes
    FROM {{ ref('stg_dailyintensities') }}
),

-- Daily step count
cte_steps AS (
    SELECT 
        id          AS user_id,  
        activityday AS activity_date, 
        steptotal   AS total_steps
    FROM {{ ref('stg_dailysteps') }}
),

-- Final merged dataset
cte_final AS (
    SELECT 
        ud.user_id,
        ud.activity_date,
        COALESCE(a.total_steps, s.total_steps, 0)                                           AS total_steps,
        COALESCE(a.total_distance, 0)                                                       AS total_distance,
        COALESCE(a.very_active_minutes, i.very_active_minutes, 0) 
            + COALESCE(a.fairly_active_minutes, i.fairly_active_minutes, 0) 
            + COALESCE(a.lightly_active_minutes, i.lightly_active_minutes, 0)              AS active_minutes,
        COALESCE(a.total_calories, c.daily_calories, 0)                                     AS total_calories
    FROM cte_all_user_dates ud
    LEFT JOIN cte_activity     a ON ud.user_id = a.user_id AND ud.activity_date = a.activity_date
    LEFT JOIN cte_calories     c ON ud.user_id = c.user_id AND ud.activity_date = c.activity_date
    LEFT JOIN cte_intensities  i ON ud.user_id = i.user_id AND ud.activity_date = i.activity_date
    LEFT JOIN cte_steps        s ON ud.user_id = s.user_id AND ud.activity_date = s.activity_date
)

SELECT * FROM cte_final
