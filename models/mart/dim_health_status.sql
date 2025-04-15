{{ config(materialized='table') }}

SELECT
    CAST(health_status_id AS NUMBER) AS health_status_id,
    health_status_name
FROM VALUES
    (1, 'Healthy'),
    (2, 'Overweight'),
    (3, 'Underweight'),
    (4, 'Obese'),
    (5, 'Unknown')
    AS t(health_status_id, health_status_name)
