CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.refund_rate` AS
SELECT
    vendor,
    COUNTIF(event_type = 'refund_issued') AS total_refunds,
    COUNT(*) AS total_orders,
    ROUND(100 * COUNTIF(event_type = 'refund_issued') / COUNT(*), 2) AS refund_rate_percent
FROM
    `commercepulsedatapack.commercepulse.cleaned_data`
WHERE event_type IN ('order_created', 'refund_issued', 'payment_succeeded')
GROUP BY vendor
ORDER BY refund_rate_percent DESC;