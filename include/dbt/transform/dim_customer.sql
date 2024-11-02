-- dim_customer.sql

WITH customer_cte AS (
    SELECT DISTINCT
        customer_id,
        join_date
    FROM {{ source('ecom', 'customers') }}
    WHERE customer_id IS NOT NULL
)

SELECT 
    customer_id,
    join_date
FROM customer_cte
