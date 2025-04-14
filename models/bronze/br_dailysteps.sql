{{ config(
    materialized = 'table',
    tags = ['bronze', 'dailysteps', 'br'],
    description = 'Daily steps data from Fitbit, cleaned and cast to standard formats.'
) }}

WITH daily_steps AS (
    SELECT * 
    FROM {{ source('RAW_LAYER', 'dailysteps_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_date('ActivityDay') }},
        {{ cast_col('StepTotal', 'number') }}
    FROM daily_steps
),

unique_cast_columns AS (
    SELECT DISTINCT * 
    FROM cast_columns
)

SELECT * 
FROM unique_cast_columns
WHERE Id IS NOT NULL
  AND ActivityDay IS NOT NULL
