-- fact_transactions.sql

WITH fact_transactions_cte AS (
    SELECT
        index AS index,
        transaction_id AS transaction_id,
        customer_id AS customer_id,
        DATE(transaction_date) AS transaction_date,
        DATE(burn_date) AS burn_date,
        transaction_status AS transaction_status,
        coupon_name AS coupon_name,
        branch_id AS branch_id,
        redemption_status AS redemption_status,
        customers_df_city_id AS city_id,
        customers_df_gender_id AS gender_id,
        branches_df_merchant_id AS merchant_id,
        customers_df_count_transactions_df_ AS count_transactions,
        customers_df_mode_transactions_df_transaction_status_ AS mode_transaction_status,
        customers_df_num_unique_transactions_df_transaction_status_ AS num_unique_transaction_status,
        customers_df_num_unique_transactions_df_month_transaction_date__ AS num_unique_month_transactions,
        branches_df_merchants_df_count_transactions_df_ AS merchant_count_transactions,
        branches_df_merchants_df_mode_transactions_df_transaction_status_ AS merchant_mode_transaction_status,
        customers_df_mode_transactions_df_month_burn_date__ AS mode_month_burn_date,
        customers_df_mode_transactions_df_weekday_burn_date__ AS mode_weekday_burn_date
    FROM {{ source('ecom', 'transactions') }}
)

SELECT
    ft.index,
    ft.transaction_id,
    dc.customer_id,
    dm.merchant_id,
    dg.gender_id,
    dci.city_id,
    dtd.id AS transaction_date_id,
    dbd.id AS burn_date_id,
    ft.transaction_status,
    ft.coupon_name,
    ft.branch_id,
    ft.redemption_status,
    ft.count_transactions,
    ft.mode_transaction_status,
    ft.num_unique_transaction_status,
    ft.num_unique_month_transactions,
    ft.merchant_count_transactions,
    ft.merchant_mode_transaction_status,
    ft.mode_month_burn_date,
    ft.mode_weekday_burn_date
FROM fact_transactions_cte ft
-- Join with dim_customer
INNER JOIN {{ ref('dim_customer') }} dc ON ft.customer_id = dc.customer_id
-- Join with dim_merchant
INNER JOIN {{ ref('dim_merchant') }} dm ON ft.merchant_id = dm.merchant_id
-- Join with dim_gender
INNER JOIN {{ ref('dim_gender') }} dg ON ft.gender_id = dg.gender_id
-- Join with dim_cities
INNER JOIN {{ ref('dim_cities') }} dci ON ft.city_id = dci.city_id
-- Join with dim_date for transaction_date
INNER JOIN {{ ref('dim_date') }} dtd ON ft.transaction_date = dtd.id
-- Join with dim_date for burn_date
-- INNER JOIN {{ ref('dim_date') }} dbd ON ft.burn_date = dbd.id
ORDER BY
    ft.transaction_date;
