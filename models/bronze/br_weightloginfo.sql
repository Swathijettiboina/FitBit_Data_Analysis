{{ config(
    materialized = 'table',
    tags = ['bronze', 'weightloginfo', 'br'],
    description = 'Merged weight log information data from Fitbit, standardized with correct data types.'
) }}

WITH weightloginfo_raw AS (
    SELECT * FROM {{ source('RAW_LAYER', 'weightloginfo_merged_3_12') }}
    UNION ALL
    SELECT * FROM {{ source('RAW_LAYER', 'weightloginfo_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'NUMBER') }},
        {{ cast_timestamp('Date') }},
        {{ cast_col_precise('WeightKg', 'NUMBER(38,7)') }},
        {{ cast_col_precise('WeightPounds', 'NUMBER(38,7)') }},
        {{ cast_col_precise('BMI', 'NUMBER(38,7)') }},
        {{ cast_col('IsManualReport', 'VARCHAR') }},
        {{ cast_col('LogId', 'NUMBER') }}
    FROM weightloginfo_raw
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND Date IS NOT NULL
)

SELECT *
FROM filtered_data
