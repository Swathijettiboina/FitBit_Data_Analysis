{{ config(
    materialized = 'table',
    tags = ['bronze', 'minutecalorieswide', 'br'],
    description = 'Minute-level calories data in wide format from Fitbit dataset, with timestamps standardized to YYYY-MM-DD HH24:MI:SS format.'
) }}

WITH minutecalories_wide AS (
    SELECT * FROM {{ source('RAW_LAYER', 'minutecalorieswide_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('ActivityHour') }},
        {{ cast_col('Calories00', 'number') }},
        {{ cast_col('Calories01', 'number') }},
        {{ cast_col('Calories02', 'number') }},
        {{ cast_col('Calories03', 'number') }},
        {{ cast_col('Calories04', 'number') }},
        {{ cast_col('Calories05', 'number') }},
        {{ cast_col('Calories06', 'number') }},
        {{ cast_col('Calories07', 'number') }},
        {{ cast_col('Calories08', 'number') }},
        {{ cast_col('Calories09', 'number') }},
        {{ cast_col('Calories10', 'number') }},
        {{ cast_col('Calories11', 'number') }},
        {{ cast_col('Calories12', 'number') }},
        {{ cast_col('Calories13', 'number') }},
        {{ cast_col('Calories14', 'number') }},
        {{ cast_col('Calories15', 'number') }},
        {{ cast_col('Calories16', 'number') }},
        {{ cast_col('Calories17', 'number') }},
        {{ cast_col('Calories18', 'number') }},
        {{ cast_col('Calories19', 'number') }},
        {{ cast_col('Calories20', 'number') }},
        {{ cast_col('Calories21', 'number') }},
        {{ cast_col('Calories22', 'number') }},
        {{ cast_col('Calories23', 'number') }},
        {{ cast_col('Calories24', 'number') }},
        {{ cast_col('Calories25', 'number') }},
        {{ cast_col('Calories26', 'number') }},
        {{ cast_col('Calories27', 'number') }},
        {{ cast_col('Calories28', 'number') }},
        {{ cast_col('Calories29', 'number') }},
        {{ cast_col('Calories30', 'number') }},
        {{ cast_col('Calories31', 'number') }},
        {{ cast_col('Calories32', 'number') }},
        {{ cast_col('Calories33', 'number') }},
        {{ cast_col('Calories34', 'number') }},
        {{ cast_col('Calories35', 'number') }},
        {{ cast_col('Calories36', 'number') }},
        {{ cast_col('Calories37', 'number') }},
        {{ cast_col('Calories38', 'number') }},
        {{ cast_col('Calories39', 'number') }},
        {{ cast_col('Calories40', 'number') }},
        {{ cast_col('Calories41', 'number') }},
        {{ cast_col('Calories42', 'number') }},
        {{ cast_col('Calories43', 'number') }},
        {{ cast_col('Calories44', 'number') }},
        {{ cast_col('Calories45', 'number') }},
        {{ cast_col('Calories46', 'number') }},
        {{ cast_col('Calories47', 'number') }},
        {{ cast_col('Calories48', 'number') }},
        {{ cast_col('Calories49', 'number') }},
        {{ cast_col('Calories50', 'number') }},
        {{ cast_col('Calories51', 'number') }},
        {{ cast_col('Calories52', 'number') }},
        {{ cast_col('Calories53', 'number') }},
        {{ cast_col('Calories54', 'number') }},
        {{ cast_col('Calories55', 'number') }},
        {{ cast_col('Calories56', 'number') }},
        {{ cast_col('Calories57', 'number') }},
        {{ cast_col('Calories58', 'number') }},
        {{ cast_col('Calories59', 'number') }}
    FROM minutecalories_wide
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND ActivityHour IS NOT NULL
)

SELECT *
FROM filtered_data
