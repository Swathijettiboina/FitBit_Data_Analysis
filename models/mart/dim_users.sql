{{ config(
    materialized='incremental',
    tags=[ 'dim_users','dim'],
    description="This table contains user-level information including the first and last activity dates.",
    unique_key='user_id',
    incremental_strategy='insert_overwrite',
    partition_by=['user_id'],
    post_hook=[
        """
        update {{ this }}
        set
            is_current = false,
            valid_to = current_timestamp
        where
            user_id in (
                select user_id
                from {{ this }}
                group by user_id
                having count(*) > 1
            )
            and is_current = true
            and valid_to is null
            and scd_key not in (
                select scd_key from {{ this }} where is_current = true
            )
        """
    ]
) }}
 
with all_user_dates as (
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
),
 
aggregated_users as (
    SELECT 
        user_id,
        MIN(activity_date) AS first_activity_recorded_date,
        MAX(activity_date) AS last_activity_recorded_date
    FROM all_user_dates
    GROUP BY user_id
)
 
select
    user_id,
    first_activity_recorded_date,
    last_activity_recorded_date,
    {{ dbt_utils.generate_surrogate_key(['user_id', 'first_activity_recorded_date', 'last_activity_recorded_date']) }} as scd_key,
    current_timestamp as valid_from,
    null as valid_to,
    true as is_current
from aggregated_users
 