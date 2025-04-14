{{ config(
    materialized='table',
    tags=['bronze', 'dailycalories', 'br'],
    description='Daily calories data from Fitbit, cleaned and cast to standard formats.'
) }}

WITH daily_calories AS (
    SELECT * FROM {{ source('RAW_LAYER', 'dailycalories_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }}, 
        {{ cast_date("ActivityDay") }}, 
        {{ cast_col('Calories', 'number') }} 
    FROM daily_calories
),

unique_cast_columns AS (
    SELECT DISTINCT * FROM cast_columns
)

SELECT * FROM unique_cast_columns
