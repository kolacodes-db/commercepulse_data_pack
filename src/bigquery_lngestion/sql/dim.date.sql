CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.dim_date` AS
SELECT
    DATE(event_date) AS order_date,
    SUM(total_amount_ngn) AS daily_sales_ngn
FROM
    `commercepulsedatapack.commercepulse.cleaned_data`
GROUP BY
    order_date
ORDER BY
    order_date;