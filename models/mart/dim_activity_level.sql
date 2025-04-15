{{ config(materialized='table',
        tags=['mart', 'dim_activity_level', 'dim']
 ) }}

SELECT
    CAST(activity_level_id AS NUMBER) AS activity_level_id,
    activity_level_name
FROM VALUES 
    (1, 'Sedentary'),
    (2, 'Lightly Active'),
    (3, 'Fairly Active'),
    (4, 'Very Active')
    AS t(activity_level_id, activity_level_name)
