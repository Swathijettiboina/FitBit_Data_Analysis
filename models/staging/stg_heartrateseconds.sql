{{ config(
    materialized = 'table',
    tags = ['stage', 'heartrate', 'br'],
    description = 'Heart rate per second data from Fitbit, merged from March and April datasets, with timestamps standardized to YYYY-MM-DD HH24:MI:SS format.'
) }}

WITH heartrate_raw AS (
    SELECT * FROM {{ source('RAW_LAYER', 'heartrate_seconds_merged_3_12') }}
    UNION ALL
    SELECT * FROM {{ source('RAW_LAYER', 'heartrate_seconds_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('Time') }},
        {{ cast_col('Value', 'number') }}
    FROM heartrate_raw
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
)

SELECT *
FROM filtered_data
