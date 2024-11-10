CREATE OR REPLACE TABLE `data-mining-warehouse-ecom.ecom_prod.dim_customer` AS (
    WITH customer_cte AS (
        SELECT DISTINCT
            customer_id,
            join_date
        FROM `data-mining-warehouse-ecom.ecom_stg.customers`
        WHERE customer_id IS NOT NULL
    )
    SELECT customer_id, join_date FROM customer_cte
);