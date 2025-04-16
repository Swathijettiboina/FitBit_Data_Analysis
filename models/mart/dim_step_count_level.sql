{{
    config(
        materialized='table',
        tags=['mart', 'step_count_level', 'dim'],
        description="This table contains the step count level dimension, which categorizes users based on their step count levels."
    )
}}
SELECT
    CAST(step_count_level_id AS NUMBER)   AS step_count_level_id,
    step_count_level                  AS step_count_level
FROM VALUES 
    (5,'No Steps'),
    (4,'Sedentary Active Steps'),
    (3,'Lightly Active Steps'),
    (2,'Moderately Active Steps'),
    (1,'Highly  Active Steps')
    AS t(step_count_level_id, step_count_level)