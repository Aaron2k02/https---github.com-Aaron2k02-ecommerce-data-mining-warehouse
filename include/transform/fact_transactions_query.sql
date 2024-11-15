CREATE OR REPLACE TABLE data-mining-warehouse-ecom.ecom_prod.fact_transactions AS
WITH fact_transactions_cte AS (
    SELECT
        t.transaction_id,
        t.customer_id,
        CAST(t.transaction_date AS DATE) AS transaction_date,
        CAST(t.burn_date AS DATE) AS burn_date,
        t.transaction_status,
        t.coupon_name,
        t.redemption_status,
        c.city_id,
        c.gender_id,
        b.merchant_id
    FROM 
        data-mining-warehouse-ecom.ecom_stg.transactions t
    LEFT JOIN data-mining-warehouse-ecom.ecom_stg.customers c ON t.customer_id = c.customer_id
    LEFT JOIN data-mining-warehouse-ecom.ecom_stg.branches b ON t.branch_id = b.branch_id
),

-- Final Fact Table with Dimensional Joins
fact_transactions_final AS (
    SELECT
        ft.transaction_id,
        dc.customer_id,
        dm.merchant_id,
        dg.gender_id,
        dci.city_id,
        ft.transaction_status,
        ft.transaction_date AS transaction_date,
        ft.burn_date AS burn_date,
        ft.coupon_name,
        ft.redemption_status
    FROM 
        fact_transactions_cte ft
    INNER JOIN data-mining-warehouse-ecom.ecom_prod.dim_customer dc ON ft.customer_id = dc.customer_id
    INNER JOIN data-mining-warehouse-ecom.ecom_prod.dim_merchant dm ON ft.merchant_id = dm.merchant_id
    INNER JOIN data-mining-warehouse-ecom.ecom_prod.dim_gender dg ON ft.gender_id = dg.gender_id
    INNER JOIN data-mining-warehouse-ecom.ecom_prod.dim_cities dci ON ft.city_id = dci.city_id
    INNER JOIN data-mining-warehouse-ecom.ecom_prod.dim_date dtd ON ft.transaction_date = CAST(dtd.date_id AS DATE)
)

-- Final ordered selection
SELECT * 
FROM fact_transactions_final
ORDER BY transaction_date;