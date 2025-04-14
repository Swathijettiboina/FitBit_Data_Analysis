{{ config(
    materialized = 'table',
    tags = ['bronze', 'sleepday', 'br'],
    description = 'Day-level sleep data from Fitbit, merged from datasets with sleep records, total minutes asleep, and time in bed.'
) }}

WITH sleepday_raw AS (
    SELECT * FROM {{ source('RAW_LAYER', 'sleepday_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('SleepDay') }},
        {{ cast_col('TotalSleepRecords', 'number') }},
        {{ cast_col('TotalMinutesAsleep', 'number') }},
        {{ cast_col('TotalTimeInBed', 'number') }}
    FROM sleepday_raw
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND SleepDay IS NOT NULL
)

SELECT *
FROM filtered_data
