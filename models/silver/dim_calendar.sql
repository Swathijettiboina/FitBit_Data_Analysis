{{
    config(
        materialized='table',
        tags=['silver', 'calendar', 'dim'],
        description='Calendar dimension with full timestamps down to the second level, and hour/minute references.'
    )
}}

WITH date_bounds AS (
    SELECT 
        DATE_TRUNC('second', MIN(CAST(activitydate AS TIMESTAMP))) AS min_datetime,
        DATE_TRUNC('second', MAX(CAST(activitydate AS TIMESTAMP))) + INTERVAL '1 second' AS max_datetime
    FROM {{ ref('br_dailyactivity') }}
),

datetime_series AS (
    SELECT min_datetime AS second_timestamp
    FROM date_bounds

    UNION ALL

    SELECT second_timestamp + INTERVAL '1 second'
    FROM datetime_series, date_bounds
    WHERE second_timestamp + INTERVAL '1 second' <= max_datetime
)

SELECT 
    -- Primary structure (in requested order)
    CAST(second_timestamp AS DATE)         AS date,
    DATE_TRUNC('hour', second_timestamp)   AS hour_timestamp,
    DATE_TRUNC('minute', second_timestamp) AS minute_timestamp,
    second_timestamp,

    -- Date and time components
    EXTRACT(YEAR FROM second_timestamp)    AS year,
    EXTRACT(MONTH FROM second_timestamp)   AS month,
    EXTRACT(DAY FROM second_timestamp)     AS day,
    EXTRACT(HOUR FROM second_timestamp)    AS hour,
    EXTRACT(MINUTE FROM second_timestamp)  AS minute,
    EXTRACT(SECOND FROM second_timestamp)  AS second,

    -- Additional context
    DAYNAME(CAST(second_timestamp AS DATE))   AS weekday_name,    
    EXTRACT(DOW FROM second_timestamp)        AS day_of_week_num,
    EXTRACT(WEEK FROM second_timestamp)       AS week_of_year

FROM datetime_series
