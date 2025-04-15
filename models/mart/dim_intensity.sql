{{ config(materialized='table') }}

SELECT
    CAST(intensity_id AS NUMBER) AS intensity_id,
    intensity_level
FROM VALUES
    (1, 'Low'),
    (2, 'Moderate'),
    (3, 'High'),
    (4, 'Very High')
    AS t(intensity_id, intensity_level)
