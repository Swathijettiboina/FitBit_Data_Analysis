{{
    config(
        materialized='incremental',
        tags=['silver', 'calendar', 'dim'],
        description='Calendar dimension with full timestamps down to the minute level, and hour/minute references.'
    )
}}

WITH date_bounds AS (
    SELECT 
        DATE_TRUNC('minute', MIN(CAST(activity_date AS TIMESTAMP))) AS min_datetime,
        DATE_TRUNC('minute', MAX(CAST(activity_date AS TIMESTAMP))) + INTERVAL '1 minute' AS max_datetime
    FROM {{ ref('int_daily_activity') }}
),

datetime_series AS (
    SELECT min_datetime AS minute_timestamp
    FROM date_bounds

    UNION ALL

    SELECT minute_timestamp + INTERVAL '1 minute'
    FROM datetime_series, date_bounds
    WHERE minute_timestamp + INTERVAL '1 minute' <= max_datetime
)

SELECT 
    -- Primary structure (in requested order)
    CAST(minute_timestamp AS DATE)         AS date,
    DATE_TRUNC('hour', minute_timestamp)   AS hour_timestamp,
    DATE_TRUNC('minute', minute_timestamp) AS minute_timestamp,

    -- Date and time components
    EXTRACT(YEAR FROM minute_timestamp)    AS year,
    EXTRACT(MONTH FROM minute_timestamp)   AS month,
    EXTRACT(DAY FROM minute_timestamp)     AS day,
    EXTRACT(HOUR FROM minute_timestamp)    AS hour,
    EXTRACT(MINUTE FROM minute_timestamp)  AS minute,

    -- Additional context
    DAYNAME(CAST(minute_timestamp AS DATE))   AS weekday_name, 
    MONTHNAME(CAST(minute_timestamp AS DATE))  AS month_name,   
    EXTRACT(DOW FROM minute_timestamp)        AS day_of_week_num,
    EXTRACT(WEEK FROM minute_timestamp)       AS week_of_year

FROM datetime_series

{% if is_incremental() %}
    WHERE minute_timestamp > (SELECT MAX(minute_timestamp) FROM {{ this }})
{% endif %}