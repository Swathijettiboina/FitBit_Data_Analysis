{{
    config(
        materialized='table',
        tags=['mart', 'personal_activity_tag', 'dim'],
        description="This table contains the personal activity tag dimension, which categorizes users based on their personal activity levels."
    )
}}

SELECT
    CAST(personal_activity_tag_id AS NUMBER)   AS personal_activity_tag_id,
    personal_activity_tag                  AS personal_activity_tag
FROM VALUES 
    (4,'Sedentary Active'),
    (3,'Lightly Active'),
    (2,'Moderately Active'),
    (1,'Extreamly Active')
    AS t(personal_activity_tag_id, personal_activity_tag)