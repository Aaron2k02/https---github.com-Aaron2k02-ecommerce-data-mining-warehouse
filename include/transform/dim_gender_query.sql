CREATE OR REPLACE TABLE `data-mining-warehouse-ecom.ecom_prod.dim_gender` AS (
    WITH gender_cte AS (
        SELECT DISTINCT
            gender_id,
            gender_name
        FROM `data-mining-warehouse-ecom.ecom_stg.genders`
        WHERE gender_id IS NOT NULL
    )
    SELECT gender_id, gender_name FROM gender_cte
);