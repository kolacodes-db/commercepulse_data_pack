CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.fact_order_daily` AS
SELECT
    DATE(event_date) AS order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_amount_ngn) AS total_sales_ngn,
    SUM(quantity) AS total_items_sold,
    COUNT(DISTINCT CASE WHEN `event_type` = 'payment_succeeded' THEN order_id END) AS successful_orders,
    COUNT(DISTINCT CASE WHEN `event_type` = 'refund_issued' THEN order_id END) AS refunded_orders,
    COUNT(DISTINCT CASE WHEN `event_type` = 'shipment_updated' THEN order_id END) AS shipped_orders
FROM `commercepulsedatapack.commercepulse.cleaned_data`
GROUP BY order_date
ORDER BY order_date;