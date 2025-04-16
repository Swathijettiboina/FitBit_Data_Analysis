{{ config(materialized='table',
    tags=["mart", "calorie_burn_level","dim"],
    description="This table contains the calorie burn level dimension, which categorizes users based on their calorie burn levels."
 ) }}

SELECT
    CAST(calorie_level_id AS NUMBER)   AS calorie_level_id,
    calorie_burn_level                  AS calorie_burn_level
FROM VALUES 
    (4,'Sedentary Calorie Burner'),
    (3,'Low Calorie Burner'),
    (2,'Moderate Calorie Burner'),
    (1,'High Calorie Burner')
    AS t(calorie_level_id, calorie_burn_level)