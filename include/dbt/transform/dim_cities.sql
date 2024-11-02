-- dim_cities.sql

WITH cities_cte AS (
    SELECT DISTINCT
        city_id,
        city_name
    FROM {{ source('ecom', 'cities') }}
    WHERE city_id IS NOT NULL
)

SELECT 
    city_id,
    city_name
FROM cities_cte
