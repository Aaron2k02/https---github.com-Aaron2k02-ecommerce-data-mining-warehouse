CREATE OR REPLACE TABLE `data-mining-warehouse-ecom.ecom_prod.dim_date` AS (
    WITH date_range AS (
        SELECT
            MIN(DATE(transaction_date)) AS min_date,
            MAX(DATE(burn_date)) AS max_date
        FROM `data-mining-warehouse-ecom.ecom_stg.transactions`
    ),
    date_array AS (
        SELECT * 
        FROM UNNEST(GENERATE_DATE_ARRAY(
            (SELECT min_date FROM date_range),
            (SELECT max_date FROM date_range),
            INTERVAL 1 DAY
        )) AS d
    )
    SELECT
        FORMAT_DATE('%F', d) AS date_id,
        d AS full_date,
        EXTRACT(YEAR FROM d) AS year,
        EXTRACT(WEEK FROM d) AS year_week,
        EXTRACT(DAY FROM d) AS year_day,
        FORMAT_DATE('%Q', d) AS fiscal_qtr,
        EXTRACT(MONTH FROM d) AS month,
        FORMAT_DATE('%B', d) AS month_name,
        FORMAT_DATE('%w', d) AS week_day,
        FORMAT_DATE('%A', d) AS day_name,
        CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END AS day_is_weekday
    FROM date_array
    ORDER BY d
);