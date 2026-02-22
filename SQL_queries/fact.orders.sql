CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.fact_orders` AS
SELECT
    event_id,
    order_id,
    customer_id,
    DATE(event_date) AS order_date,
    total_amount,
    total_amount_ngn,
    currency,
    `event_type`,
    CASE
        WHEN `event_type` = 'order_created' THEN 'Transaction Pending'
        WHEN `event_type` = 'historical_order' THEN 'Transaction Completed'
        WHEN `event_type` = 'payment_succeeded' THEN 'Transaction Successful'
        WHEN `event_type` = 'refund_issued' THEN 'Transaction Failed'
        WHEN `event_type` = 'shipment_updated' THEN 'Transaction Delayed'
        ELSE 'Not Applicable'
    END AS transaction_summary
FROM `commercepulsedatapack.commercepulse.cleaned_data`;