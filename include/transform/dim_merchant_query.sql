CREATE OR REPLACE TABLE `data-mining-warehouse-ecom.ecom_prod.dim_merchant` AS (
    WITH merchant_cte AS (
        SELECT DISTINCT
            merchant_id,
            merchant_name
        FROM `data-mining-warehouse-ecom.ecom_stg.merchants`
        WHERE merchant_id IS NOT NULL
    )
    SELECT merchant_id, merchant_name FROM merchant_cte
);