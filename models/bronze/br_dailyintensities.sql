{{ config(
    materialized='table',
    tags=['bronze', 'dailyintensities', 'intensities'],
    description='Daily intensities data from Fitbit, cleaned and cast to standard formats.'
) }}

WITH daily_intensities AS (
    SELECT * FROM {{ source('RAW_LAYER', 'dailyintensities_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_date("ActivityDay") }},
        {{ cast_col('SedentaryMinutes', 'number') }},
        {{ cast_col('LightlyActiveMinutes', 'number') }},
        {{ cast_col('FairlyActiveMinutes', 'number') }},
        {{ cast_col('VeryActiveMinutes', 'number') }},
        {{ cast_col('SedentaryActiveDistance', 'number') }},
        {{ cast_col('LightActiveDistance', 'number') }},
        {{ cast_col('ModeratelyActiveDistance', 'number') }},
        {{ cast_col('VeryActiveDistance', 'number') }}
    FROM daily_intensities
),

unique_cast_columns AS (
    SELECT DISTINCT * FROM cast_columns
)

SELECT * 
FROM unique_cast_columns
WHERE Id IS NOT NULL 
  AND ActivityDay IS NOT NULL
