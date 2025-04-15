{{ config(materialized='table') }}

SELECT
    CAST(workout_intensity_id AS NUMBER) AS workout_intensity_id,
    workout_intensity_name
FROM VALUES
    (1, 'Light'),
    (2, 'Moderate'),
    (3, 'Intense'),
    (4, 'Extreme')
    AS t(workout_intensity_id, workout_intensity_name)
