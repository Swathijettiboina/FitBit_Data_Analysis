{{ config(
    materialized = 'table',
    tags = ['bronze', 'minuteintensitiesnarrow', 'br'],
    description = 'Minute-level intensity data from Fitbit merged from  datasets, with timestamps standardized to YYYY-MM-DD HH24:MI:SS format.'
) }}

WITH intensity_raw AS (
    SELECT * FROM {{ source('RAW_LAYER', 'minuteintensitiesnarrow_merged_3_12') }}
    UNION ALL
    SELECT * FROM {{ source('RAW_LAYER', 'minuteintensitiesnarrow_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('ActivityMinute') }},
        {{ cast_col('Intensity', 'number') }}
    FROM intensity_raw
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND ActivityMinute IS NOT NULL
)

SELECT *
FROM filtered_data
