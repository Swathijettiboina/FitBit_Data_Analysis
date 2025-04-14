{{ config(
    materialized = 'table',
    tags = ['bronze', 'minutestepswide', 'br'],
    description = 'Minute-level steps wide data from Fitbit, merged from datasets with hourly steps recorded.'
) }}

WITH minutestepswide_raw AS (
    SELECT * FROM {{ source('RAW_LAYER', 'minutestepswide_merged_4_12') }}
),

cast_columns AS (
    SELECT
        {{ cast_col('Id', 'number') }},
        {{ cast_timestamp('ActivityHour') }},
        {{ cast_col('Steps00', 'number') }},
        {{ cast_col('Steps01', 'number') }},
        {{ cast_col('Steps02', 'number') }},
        {{ cast_col('Steps03', 'number') }},
        {{ cast_col('Steps04', 'number') }},
        {{ cast_col('Steps05', 'number') }},
        {{ cast_col('Steps06', 'number') }},
        {{ cast_col('Steps07', 'number') }},
        {{ cast_col('Steps08', 'number') }},
        {{ cast_col('Steps09', 'number') }},
        {{ cast_col('Steps10', 'number') }},
        {{ cast_col('Steps11', 'number') }},
        {{ cast_col('Steps12', 'number') }},
        {{ cast_col('Steps13', 'number') }},
        {{ cast_col('Steps14', 'number') }},
        {{ cast_col('Steps15', 'number') }},
        {{ cast_col('Steps16', 'number') }},
        {{ cast_col('Steps17', 'number') }},
        {{ cast_col('Steps18', 'number') }},
        {{ cast_col('Steps19', 'number') }},
        {{ cast_col('Steps20', 'number') }},
        {{ cast_col('Steps21', 'number') }},
        {{ cast_col('Steps22', 'number') }},
        {{ cast_col('Steps23', 'number') }},
        {{ cast_col('Steps24', 'number') }},
        {{ cast_col('Steps25', 'number') }},
        {{ cast_col('Steps26', 'number') }},
        {{ cast_col('Steps27', 'number') }},
        {{ cast_col('Steps28', 'number') }},
        {{ cast_col('Steps29', 'number') }},
        {{ cast_col('Steps30', 'number') }},
        {{ cast_col('Steps31', 'number') }},
        {{ cast_col('Steps32', 'number') }},
        {{ cast_col('Steps33', 'number') }},
        {{ cast_col('Steps34', 'number') }},
        {{ cast_col('Steps35', 'number') }},
        {{ cast_col('Steps36', 'number') }},
        {{ cast_col('Steps37', 'number') }},
        {{ cast_col('Steps38', 'number') }},
        {{ cast_col('Steps39', 'number') }},
        {{ cast_col('Steps40', 'number') }},
        {{ cast_col('Steps41', 'number') }},
        {{ cast_col('Steps42', 'number') }},
        {{ cast_col('Steps43', 'number') }},
        {{ cast_col('Steps44', 'number') }},
        {{ cast_col('Steps45', 'number') }},
        {{ cast_col('Steps46', 'number') }},
        {{ cast_col('Steps47', 'number') }},
        {{ cast_col('Steps48', 'number') }},
        {{ cast_col('Steps49', 'number') }},
        {{ cast_col('Steps50', 'number') }},
        {{ cast_col('Steps51', 'number') }},
        {{ cast_col('Steps52', 'number') }},
        {{ cast_col('Steps53', 'number') }},
        {{ cast_col('Steps54', 'number') }},
        {{ cast_col('Steps55', 'number') }},
        {{ cast_col('Steps56', 'number') }},
        {{ cast_col('Steps57', 'number') }},
        {{ cast_col('Steps58', 'number') }},
        {{ cast_col('Steps59', 'number') }}
    FROM minutestepswide_raw
),

filtered_data AS (
    SELECT DISTINCT *
    FROM cast_columns
    WHERE Id IS NOT NULL
      AND ActivityHour IS NOT NULL
)

SELECT *
FROM filtered_data
