CREATE OR REPLACE TABLE `data-mining-warehouse-ecom.ecom_prod.fact_transactions` AS
    WITH fact_transactions_cte AS (
        SELECT
            index,
            transaction_id,
            customer_id,
            transaction_date,
            burn_date,
            transaction_status,
            coupon_name,
            branch_id,
            redemption_status,
            customers_df_city_id AS city_id,
            customers_df_gender_id AS gender_id,
            branches_df_merchant_id AS merchant_id,
            customers_df_count_transactions_df AS count_transactions,
            customers_df_mode_transactions_df_transaction_status AS mode_transaction_status,
            customers_df_num_unique_transactions_df_transaction_status AS num_unique_transaction_status,
            customers_df_num_unique_transactions_df_month_transaction_date AS num_unique_month_transactions,
            branches_df_merchants_df_count_transactions_df AS merchant_count_transactions,
            branches_df_merchants_df_mode_transactions_df_transaction_status AS merchant_mode_transaction_status,
            customers_df_mode_transactions_df_month_burn_date AS mode_month_burn_date,
            customers_df_mode_transactions_df_weekday_burn_date AS mode_weekday_burn_date
        FROM `data-mining-warehouse-ecom.ecom_stg.transactions`
    )
SELECT
    ft.index,
    ft.transaction_id,
    dc.customer_id,
    dm.merchant_id,
    dg.gender_id,
    dci.city_id,
    dtd.id AS transaction_date_id,
    ft.count_transactions,
    ft.mode_transaction_status,
    ft.num_unique_transaction_status,
    ft.num_unique_month_transactions,
    ft.merchant_count_transactions,
    ft.merchant_mode_transaction_status,
    ft.mode_month_burn_date,
    ft.mode_weekday_burn_date
FROM fact_transactions_cte ft
INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_customer` dc ON ft.customer_id = dc.customer_id
INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_merchant` dm ON ft.merchant_id = dm.merchant_id
INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_gender` dg ON ft.gender_id = dg.gender_id
INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_cities` dci ON ft.city_id = dci.city_id
INNER JOIN `data-mining-warehouse-ecom.ecom_prod.dim_date` dtd ON ft.transaction_date = CAST(dtd.id AS DATE)
ORDER BY ft.transaction_date