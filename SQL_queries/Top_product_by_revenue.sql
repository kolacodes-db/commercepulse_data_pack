CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.top_products` AS
SELECT
    sku,
    SUM(sales_total_ngn) AS total_revenue_ngn,
    SUM(quantity) AS total_quantity_sold
FROM
    `commercepulsedatapack.commercepulse.cleaned_data`
GROUP BY sku
ORDER BY total_revenue_ngn DESC
LIMIT 20;