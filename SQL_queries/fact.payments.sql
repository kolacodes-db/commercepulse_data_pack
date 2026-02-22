
-- FACT: Payments with Transaction Summary

CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.fact_payments` AS
SELECT
    event_id,
    order_id,
    DATE(event_date) AS payment_date,
    total_amount_ngn AS amount_paid,
    currency,
    event_type,
    CASE
        WHEN event_type = 'order_created' THEN 'Transaction pending'
        WHEN event_type = 'shipment_updated' THEN 'Transaction in progress'
        WHEN event_type = 'historical_order' THEN 'Transaction completed'
        WHEN event_type = 'payment_succeeded' THEN 'Transaction successful'
        WHEN event_type = 'refund_issued' THEN 'Transaction failed'
        ELSE 'Transaction unknown'
    END AS transaction_summary
FROM (
    SELECT DISTINCT
        event_id,
        order_id,
        event_date,
        total_amount_ngn,
        currency,
        event_type
    FROM
        `commercepulsedatapack.commercepulse.cleaned_data`
)