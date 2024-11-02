-- dim_gender.sql

WITH gender_cte AS (
    SELECT DISTINCT
        gender_id,
        gender_name
    FROM {{ source('ecom', 'genders') }}
    WHERE gender_id IS NOT NULL
)

SELECT 
    gender_id,
    gender_name
FROM gender_cte
