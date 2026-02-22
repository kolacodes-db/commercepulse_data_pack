CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.daily_revenue` AS
SELECT
    DATE(event_date) AS order_date,
    SUM(total_amount_ngn) AS daily_gross_revenue_ngn,
    SUM(sales_total_ngn) AS daily_net_revenue_ngn
FROM
    `commercepulsedatapack.commercepulse.cleaned_data`
GROUP BY
    order_date
ORDER BY
    order_date;