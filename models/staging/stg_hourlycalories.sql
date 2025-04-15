{{ config(
    materialized = 'table',
    tags = ['stage', 'hourlycalories', 'br'],
    description = 'Calories data per hour from Fitbit, merged from datasets, with timestamps standardized to YYYY-MM-DD HH24:MI:SS format.'
) }}

WITH calories_raw AS (
    SELECT * FROM {{ source('RAW_LAYER', 'hourlycalories_merged_3_12') }}
    UNION ALL
    SELECT * FROM {{ source('RAW_LAYER', 'hourlycalories_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('ActivityHour') }},
        {{ cast_col('Calories', 'number') }}
    FROM calories_raw
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND ActivityHour IS NOT NULL
)

SELECT *
FROM filtered_data
