{{
    config(
        materialized='table',
        tags=['silver', 'dim_users'],
        description='Dimension table to store unique users with their first and last activity dates.'
    )
}}

WITH cte_all_users AS (
    -- Collect all unique user IDs and activity dates from activity-related tables
    SELECT 
        id              AS user_id, 
        activitydate    AS activityday
    FROM {{ ref('br_dailyactivity') }}
    
    UNION
    
    SELECT 
        id AS user_id, 
        activityday
    FROM {{ ref('br_dailycalories') }}
    
    UNION
    
    SELECT 
        id AS user_id, 
        activityday
    FROM {{ ref('br_dailyintensities') }}
    
    UNION
    
    SELECT 
        id AS user_id, 
        activityday
    FROM {{ ref('br_dailysteps') }}
),

user_activity_dates AS (
    SELECT 
        user_id,
        MIN(activityday) AS first_activity_recorded_date,
        MAX(activityday) AS last_activity_recorded_date
    FROM cte_all_users
    GROUP BY user_id
)

-- Final select with row numbering, ensuring the desired order
SELECT 
    ROW_NUMBER() OVER (ORDER BY user_id) AS row_number, 
    user_id,                                           
    first_activity_recorded_date,                              
    last_activity_recorded_date                                
FROM user_activity_dates
