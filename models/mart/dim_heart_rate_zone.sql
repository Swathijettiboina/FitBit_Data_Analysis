{{
    config(
        materialized='table',
        tags=['mart', 'heart_rate_zone', 'dim'],
        description="This table contains the heart rate zone dimension, which categorizes users based on their heart rate zones."
    )
}}
SELECT
    CAST(heart_rate_zone_id AS NUMBER)   AS heart_rate_zone_id,
    heart_rate_zone                  AS heart_rate_zone
FROM VALUES 
    (1,'Resting'),
    (2,'Warm-up'),
    (3,'Fat Burn'),
    (4,'Cardio'),
    (5,'Peak'),
    (6,'Max'),
    (7,'Unknown')
    AS t(heart_rate_zone_id, heart_rate_zone)
