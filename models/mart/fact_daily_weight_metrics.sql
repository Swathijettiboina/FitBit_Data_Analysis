{{
    config(
        materialized='table',
        unique_key='id',
        tags=['daily_weight_metrics',"fact", "weight",'mart,' "metrics"],
        description="This table contains daily weight metrics for users, including weight, BMI."
    )
}}

SELECT
    u.user_id,
    c.date AS activity_date,
    wm.avg_weight_kg,
    wm.avg_bmi_score,
    hs.health_status_id,
    wm.weight_trend,
    wm.days_since_previous,
    wm.prev_weight,
    wm.prev_date
FROM {{ ref('int_daily_weight_metrics') }} wm
JOIN {{ ref('dim_users') }} u ON wm.user_id = u.user_id
JOIN {{ ref('dim_calendar') }} c ON wm.record_date = c.date
JOIN {{ ref('dim_health_status') }} hs ON wm.health_status = hs.health_status