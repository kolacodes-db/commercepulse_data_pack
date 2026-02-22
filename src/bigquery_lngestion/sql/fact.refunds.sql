CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.fact_refunds` AS
SELECT
    event_id,
    order_id,
    customer_id,
    DATE(event_date) AS refund_date,
    total_amount_ngn AS refund_amount,
    `event_type`,
    CASE
        WHEN `event_type` = 'refund_issued' THEN 'Customer Refunded'
        ELSE 'Not Applicable'
    END AS refund_status
FROM `commercepulsedatapack.commercepulse.cleaned_data`;