-- test: no duplicate minute entries
SELECT user_id, activity_minute, COUNT(*)
FROM {{ ref('report_fitbit_analysis') }}
GROUP BY user_id, activity_minute
HAVING COUNT(*) > 1
