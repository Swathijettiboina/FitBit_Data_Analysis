{{ config(
    materialized = 'table',
    tags = ['stage', 'minutemetsnarrow', 'br'],
    description = 'Merged intensity data from Fitbit, with standardized timestamps and METs values.'
) }}

WITH intensities_raw AS (
    SELECT * FROM {{ source('RAW_LAYER', 'minutemetsnarrow_merged_3_12') }}
    UNION ALL
    SELECT * FROM {{ source('RAW_LAYER', 'minutemetsnarrow_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('ActivityMinute') }},
        {{ cast_col('METs', 'number') }}
    FROM intensities_raw
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND ActivityMinute IS NOT NULL
)
SELECT *
FROM filtered_data
