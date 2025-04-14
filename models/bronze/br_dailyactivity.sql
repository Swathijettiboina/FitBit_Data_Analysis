{{ config(
    materialized='table',
    tags=['bronze', 'dailyactivity'],
    description='Daily activity data from Fitbit merged with user data and combining the two files into one.'
) }}

WITH daily_activity AS (
    SELECT * FROM {{ source('RAW_LAYER', 'dailyactivity_merged_3_12') }}
    UNION ALL
    SELECT * FROM {{ source('RAW_LAYER', 'dailyactivity_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col("Id", "number") }},
        {{ cast_date("ActivityDate") }},
        {{ cast_col("TotalSteps", "number") }},
        {{ cast_col("TotalDistance", "number") }},
        {{ cast_col("TrackerDistance", "number") }},
        {{ cast_col("LoggedActivitiesDistance", "number") }},
        {{ cast_col("VeryActiveDistance", "number") }},
        {{ cast_col("ModeratelyActiveDistance", "number") }},
        {{ cast_col("LightActiveDistance", "number") }},
        {{ cast_col("SedentaryActiveDistance", "number") }},
        {{ cast_col("VeryActiveMinutes", "number") }},
        {{ cast_col("FairlyActiveMinutes", "number") }},
        {{ cast_col("LightlyActiveMinutes", "number") }},
        {{ cast_col("SedentaryMinutes", "number") }},
        {{ cast_col("Calories", "number") }}
    FROM daily_activity
),

unique_cast_columns AS (
    SELECT DISTINCT * FROM cast_columns
)

SELECT 
    * 
FROM 
    unique_cast_columns
WHERE
    Id IS NOT NULL 

