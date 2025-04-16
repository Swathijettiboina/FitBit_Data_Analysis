{{
    config(
        materialized='table',
        unique_key='health_status_id',
        tags=['mart', 'dim', 'health_status'],
        description="This table contains the health status of users based on their Fitbit data."
    )
}}

SELECT
    CAST(health_status_id AS NUMBER)   AS health_status_id,
    health_status                      AS health_status
FROM VALUES 
    (1,'Healthy'),
    (2,'Underweight'),
    (3,'Overweight'),
    (4,'Obese'),
    (5,'Unknown')
    AS t(health_status_id, health_status)