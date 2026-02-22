CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.payment_success_rate` AS
SELECT
    vendor,
    COUNTIF(event_type = 'payment_succeeded') AS successful_payments,
    COUNT(*) AS total_payments,
    ROUND(100 * COUNTIF(event_type = 'payment_succeeded') / COUNT(*), 2) AS success_rate_percent
FROM
    `commercepulsedatapack.commercepulse.cleaned_data`
WHERE event_type IN ('payment_succeeded', 'order_created', 'payment_failed')
GROUP BY
    vendor
ORDER BY
    success_rate_percent DESC;