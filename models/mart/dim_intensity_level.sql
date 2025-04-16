{{
    config(
        materialized='table',
        tags=['mart', 'intensity_level', 'dim'],
        description="This table contains the intensity level dimension, which categorizes users based on their activity intensity levels."
    )
}}

SELECT
    CAST(intensity_level_id AS NUMBER)   AS intensity_level_id,
    intensity_level                      AS intensity_level
FROM VALUES 
    (4,'Very Low Intensity'),
    (3,'Low Intensity'),
    (2,'Moderate Intensity'),
    (1,'High Intensity')
    AS t(intensity_level_id, intensity_level)