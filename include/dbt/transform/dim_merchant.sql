-- dim_merchant.sql

WITH merchant_cte AS (
    SELECT DISTINCT
        merchant_id,
        merchant_name
    FROM {{ source('ecom', 'merchants') }}
    WHERE merchant_id IS NOT NULL
)

SELECT 
    merchant_id,
    merchant_name
FROM merchant_cte
