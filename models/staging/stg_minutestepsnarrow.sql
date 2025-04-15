{{ config(
    materialized = 'table',
    tags = ['stage', 'minutestepsnarrow', 'br'],
    description = 'Minute-level steps data from Fitbit, merged from datasets, with timestamps standardized to YYYY-MM-DD HH24:MI:SS format.'
) }}

WITH minutesteps_raw AS (
    -- Union the two tables
    SELECT * FROM {{ source('RAW_LAYER', 'minutestepsnarrow_merged_3_12') }}
    UNION ALL
    SELECT * FROM {{ source('RAW_LAYER', 'minutestepsnarrow_merged_4_12') }}
),

cast_columns AS (
    -- Cast columns to appropriate types
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('ActivityMinute') }},
        {{ cast_col('Steps', 'number') }}
    FROM minutesteps_raw
),

filtered_data AS (
    -- Remove records with NULL values in important columns
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND ActivityMinute IS NOT NULL
)

SELECT *
FROM filtered_data