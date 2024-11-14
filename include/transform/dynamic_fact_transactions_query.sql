CREATE OR REPLACE TABLE `data-mining-warehouse-ecom.ecom_prod.fact_transactions` AS
WITH fact_transactions_cte AS (
    SELECT
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        t.burn_date,
        t.transaction_status,
        t.coupon_name,
        t.redemption_status,
        c.city_id,
        c.gender_id,
        b.merchant_id,

        -- Feature Engineering using SQL transformations

        -- 1. Count of transactions per customer
        COUNT(t.transaction_id) OVER (PARTITION BY t.customer_id) AS count_transactions,

        -- 2. Mode of transaction status per customer
        APPROX_TOP_COUNT(t.transaction_status, 1) OVER (PARTITION BY t.customer_id)[OFFSET(0)].value AS mode_transaction_status,

        -- 3. Number of unique transaction statuses per customer
        COUNT(DISTINCT t.transaction_status) OVER (PARTITION BY t.customer_id) AS num_unique_transaction_status,

        -- 4. Number of unique months of transactions per customer
        COUNT(DISTINCT EXTRACT(MONTH FROM t.transaction_date)) OVER (PARTITION BY t.customer_id) AS num_unique_month_transactions,

        -- 5. Count of transactions per merchant
        COUNT(t.transaction_id) OVER (PARTITION BY b.merchant_id) AS merchant_count_transactions,

        -- 6. Mode of transaction status per merchant
        APPROX_TOP_COUNT(t.transaction_status, 1) OVER (PARTITION BY b.merchant_id)[OFFSET(0)].value AS merchant_mode_transaction_status,

        -- 7. Mode of month from burn date per customer
        APPROX_TOP_COUNT(EXTRACT(MONTH FROM t.burn_date), 1) OVER (PARTITION BY t.customer_id)[OFFSET(0)].value AS mode_month_burn_date,

        -- 8. Mode of weekday from burn date per customer
        APPROX_TOP_COUNT(EXTRACT(DAYOFWEEK FROM t.burn_date), 1) OVER (PARTITION BY t.customer_id)[OFFSET(0)].value AS mode_weekday_burn_date

    FROM 
        `data-mining-warehouse-ecom.ecom_stg.transactions` t
    LEFT JOIN `data-mining-warehouse-ecom.ecom_stg.customers` c ON t.customer_id = c.customer_id
    LEFT JOIN `data-mining-warehouse-ecom.ecom_stg.branches` b ON t.branch_id = b.branch_id
),

-- Final Fact Table with Dimensional Joins
fact_transactions_final AS (
    SELECT
        ft.transaction_id,
        dc.customer_id,
        dm.merchant_id,
        dg.gender_id,
        dci.city_id,
        dtd.date_id AS transaction_date_id,
        ft.transaction_status,
        ft.burn_date,
        ft.coupon_name,
        ft.redemption_status,
        ft.count_transactions,
        ft.mode_transaction_status,
        ft.num_unique_transaction_status,
        ft.num_unique_month_transactions,
        ft.merchant_count_transactions,
        ft.merchant_mode_transaction_status,
        ft.mode_month_burn_date,
        ft.mode_weekday_burn_date
    FROM 
        fact_transactions_cte ft
    INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_customer` dc ON ft.customer_id = dc.customer_id
    INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_merchant` dm ON ft.merchant_id = dm.merchant_id
    INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_gender` dg ON ft.gender_id = dg.gender_id
    INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_cities` dci ON ft.city_id = dci.city_id
    INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_date` dtd ON ft.transaction_date = CAST(dtd.date_id AS DATE)
)

-- Final ordered selection
SELECT * 
FROM fact_transactions_final
ORDER BY transaction_date_id;
