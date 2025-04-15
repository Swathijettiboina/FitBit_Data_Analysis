{{ config(
    materialized = 'table',
    tags         = ['mart', 'fact_hourlyactivity'],
    description  = 'Hourly activity data from Fitbit merged with user data, combining calories, steps, intensity, and average heart rate per user per hour.'
) }}

-- Get distinct user IDs from all hourly data tables
WITH cte_all_users AS (
    SELECT 
        DISTINCT user_id
    FROM {{ref('dim_users')}}
),

-- Get all hourly timestamps from dim_calendar
cte_all_hours AS (
    SELECT DISTINCT
        hour_timestamp AS activity_hour 
    FROM {{ ref('dim_calendar') }}
),

-- Generate all combinations of users and hours
cte_all_user_hours AS (
    SELECT 
        u.user_id,
        d.activity_hour
    FROM cte_all_users u
    CROSS JOIN cte_all_hours d
),

-- Hourly calories data
cte_calories AS (
    SELECT 
        id           AS user_id,  
        activityhour AS activity_hour, 
        calories     AS calories
    FROM {{ ref('stg_hourlycalories') }}
),

-- Hourly steps data
cte_steps AS (
    SELECT 
        id           AS user_id,  
        activityhour AS activity_hour, 
        steptotal   AS steps
    FROM {{ ref('stg_hourlysteps') }}
),

-- Hourly intensity data
cte_intensities AS (
    SELECT 
        id           AS user_id,  
        activityhour AS activity_hour, 
        totalintensity    AS total_intensity,
        averageintensity    AS average_intensity,
    FROM {{ ref('stg_hourlyintensities') }}
),

-- Hourly average heart rate
cte_heartrate AS (
    SELECT
        id                        AS user_id,
        DATE_TRUNC('HOUR', time) AS activity_hour,
        AVG(value)               AS avg_heart_rate
    FROM {{ ref('stg_heartrateseconds') }}
    GROUP BY user_id, activity_hour
),

-- Final dataset with left joins to preserve all user-hour combinations
cte_final AS (
    SELECT 
        uh.user_id, 
        uh.activity_hour,
        COALESCE(c.calories, 0)         AS calories,
        COALESCE(s.steps, 0)            AS steps,
        COALESCE(i.total_intensity, 0)        AS intensity,
        COALESCE(i.average_intensity, 0)      AS average_intensity,
        COALESCE(h.avg_heart_rate, 0)   AS avg_heart_rate
    FROM cte_all_user_hours uh
    LEFT JOIN cte_calories   c ON uh.user_id = c.user_id AND uh.activity_hour = c.activity_hour
    LEFT JOIN cte_steps      s ON uh.user_id = s.user_id AND uh.activity_hour = s.activity_hour
    LEFT JOIN cte_intensities i ON uh.user_id = i.user_id AND uh.activity_hour = i.activity_hour
    LEFT JOIN cte_heartrate  h ON uh.user_id = h.user_id AND uh.activity_hour = h.activity_hour
)

SELECT * FROM cte_final
