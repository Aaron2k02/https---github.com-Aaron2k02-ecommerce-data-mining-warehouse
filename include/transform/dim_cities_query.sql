CREATE OR REPLACE TABLE `data-mining-warehouse-ecom.ecom_prod.dim_cities` AS (
    WITH cities_cte AS (
        SELECT DISTINCT
            city_id,
            city_name
        FROM `data-mining-warehouse-ecom.ecom_stg.cities`
        WHERE city_id IS NOT NULL
    )
    SELECT city_id, city_name FROM cities_cte
);