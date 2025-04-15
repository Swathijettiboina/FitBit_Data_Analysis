{{ config(
    materialized='incremental',
    tags=['mart', 'dim_users','dim'],
    description="This table contains user-level information including the first and last activity dates.",
    unique_key='user_id'
) }}

WITH all_user_dates AS (
    SELECT 
        user_id, 
        activity_date
    FROM {{ ref('int_daily_activity') }}
    {% if is_incremental() %}
        WHERE activity_date > (SELECT MAX(last_activity_recorded_date) FROM {{ this }})
    {% endif %}
    
    UNION ALL
    
    SELECT 
        user_id, 
        activity_date
    FROM {{ ref('int_daily_physical_metrics') }}
    {% if is_incremental() %}
        WHERE activity_date > (SELECT MAX(last_activity_recorded_date) FROM {{ this }})
    {% endif %}
    
    UNION ALL
    
    SELECT 
        user_id, 
        CAST(activity_day AS DATE) AS activity_date
    FROM {{ ref('int_daily_sleep_activity') }}
    {% if is_incremental() %}
        WHERE activity_date > (SELECT MAX(last_activity_recorded_date) FROM {{ this }})
    {% endif %}
    
    UNION ALL
    
    SELECT 
        user_id, 
        record_date AS activity_date
    FROM {{ ref('int_daily_weight_metrics') }}
    {% if is_incremental() %}
        WHERE activity_date > (SELECT MAX(last_activity_recorded_date) FROM {{ this }})
    {% endif %}
)

SELECT 
    user_id,
    MIN(activity_date) AS first_activity_recorded_date,
    MAX(activity_date) AS last_activity_recorded_date
FROM all_user_dates
GROUP BY user_id
