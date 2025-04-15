{{ config(
    materialized = 'table',
    tags = ['stage', 'minutesleep', 'br'],
    description = 'Minute-level sleep data from Fitbit, merged from datasets, with timestamps standardized to YYYY-MM-DD HH24:MI:SS format.'
) }}

WITH minutesleep_raw AS (
    SELECT * FROM {{ source('RAW_LAYER', 'minutesleep_merged_3_12') }}
    UNION ALL
    SELECT * FROM {{ source('RAW_LAYER', 'minutesleep_merged_4_12') }}
),
cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('date') }},
        {{ cast_col('value', 'number') }},
        {{ cast_col('logId', 'number') }}
    FROM minutesleep_raw
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND date IS NOT NULL
)

SELECT *
FROM filtered_data
