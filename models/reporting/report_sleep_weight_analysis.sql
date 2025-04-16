{{
    config(
        materialized='table',
        tags=['reporting', 'sleep_weight_analysis', 'report', 'analysis'],
        description="This report combines daily sleep activity and weight metrics for users, providing insights into their sleep quality and weight trends."
    )
}}
SELECT
    u.user_id,
    c.date AS activity_date,
    sa.total_sleep_records,
    sa.total_minutes_asleep,
    sa.total_time_in_bed,
    sa.sleep_efficiency,
    hs.health_status,

    COALESCE(wgt.avg_weight_kg, 0) AS avg_weight_kg,
    COALESCE(wgt.avg_bmi_score, 0) AS avg_bmi_score,
    COALESCE(wgt.weight_trend, 'No trend') AS weight_trend,
    COALESCE(wgt.days_since_previous, 0) AS days_since_previous,
    COALESCE(wgt.prev_weight, 0) AS prev_weight
FROM
    {{ ref('fact_daily_sleep_activity') }} sa
LEFT JOIN 
    {{ ref('fact_daily_weight_metrics') }} wgt 
    ON sa.user_id = wgt.user_id AND sa.activity_date = wgt.activity_date
LEFT JOIN 
    {{ ref('dim_users') }} u 
    ON sa.user_id = u.user_id
LEFT JOIN 
    {{ ref('dim_calendar') }} c 
    ON sa.activity_date = c.date
LEFT JOIN 
   {{ref('dim_health_status') }} hs 
   ON wgt.health_status_id = hs.health_status_id