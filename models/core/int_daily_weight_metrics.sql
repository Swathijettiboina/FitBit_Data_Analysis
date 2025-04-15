{{
  config(
    materialized = 'table',
    tags        = ['weight_metrics', 'core'],
    description = 'Weight analysis with complete trend calculation'
  )
}}

WITH daily_weight AS (
    SELECT
        id                  AS user_id,
        DATE(date)          AS record_date,
        AVG(weightkg)       AS avg_weight_kg,
        AVG(bmi)            AS avg_bmi_score
    FROM {{ ref('stg_weightloginfo') }}
    GROUP BY user_id, record_date
),

weight_with_lag AS (
    SELECT
        user_id,
        record_date,
        avg_weight_kg,
        avg_bmi_score,
        LAG(record_date) OVER (PARTITION BY user_id ORDER BY record_date) AS prev_date,
        LAG(avg_weight_kg) OVER (PARTITION BY user_id ORDER BY record_date) AS prev_weight
    FROM daily_weight
),

weight_with_trend AS (
    SELECT
        *,
        CASE
            WHEN prev_date IS NULL THEN 0  -- First record
            ELSE DATEDIFF('day', prev_date, record_date)
        END AS days_since_previous,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY record_date) AS record_num
    FROM weight_with_lag
)

SELECT
    user_id,
    record_date,
    avg_weight_kg,
    avg_bmi_score,
    
    CASE
        WHEN avg_bmi_score BETWEEN 18.5 AND 24.9 THEN 'Healthy'
        WHEN avg_bmi_score < 18.5 THEN 'Underweight'
        WHEN avg_bmi_score BETWEEN 25 AND 29.9 THEN 'Overweight'
        WHEN avg_bmi_score >= 30 THEN 'Obese'
        ELSE 'Unknown'
    END AS health_status,
    
    CASE
        WHEN record_num = 1 THEN 'First Record'
        WHEN days_since_previous > 7 THEN CONCAT('Gap: ', days_since_previous, ' days')
        WHEN avg_weight_kg < prev_weight THEN 'Decreasing'
        WHEN avg_weight_kg > prev_weight THEN 'Increasing'
        ELSE 'Stable'
    END AS weight_trend,
    
    -- columns with first record adjustments
    days_since_previous,
    CASE
        WHEN record_num = 1 THEN avg_weight_kg  
        ELSE prev_weight
    END AS prev_weight,
    CASE
        WHEN record_num = 1 THEN DATEADD(day, -1, record_date)  
        ELSE prev_date
    END AS prev_date

FROM weight_with_trend
ORDER BY user_id, record_date