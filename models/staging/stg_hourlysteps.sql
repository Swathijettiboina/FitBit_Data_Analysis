{{ config(
    materialized = 'table',
    tags = ['stage', 'hourlysteps', 'br'],
    description = 'Step count per hour from Fitbit, merged from datasets, with timestamps standardized to YYYY-MM-DD HH24:MI:SS format.'
) }}

WITH steps_raw AS (
    SELECT * FROM {{ source('RAW_LAYER', 'hourlysteps_merged_3_12') }}
    UNION ALL
    SELECT * FROM {{ source('RAW_LAYER', 'hourlysteps_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('ActivityHour') }},
        {{ cast_col('StepTotal', 'number') }}
    FROM steps_raw
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND ActivityHour IS NOT NULL
)

SELECT *
FROM filtered_data
