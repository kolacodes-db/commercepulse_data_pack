CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.avg_order_to_payment_time` AS
WITH order_events AS (
    SELECT
        order_id,
        MIN(event_date) AS order_created_date
    FROM
        `commercepulsedatapack.commercepulse.cleaned_data`
    WHERE event_type = 'order_created'
    GROUP BY order_id
),
payment_events AS (
    SELECT
        order_id,
        MIN(event_date) AS payment_date
    FROM
        `commercepulsedatapack.commercepulse.cleaned_data`
    WHERE event_type = 'payment_succeeded'
    GROUP BY order_id
)
SELECT
    o.order_id,
    TIMESTAMP_DIFF(p.payment_date, o.order_created_date, MINUTE) AS minutes_to_payment
FROM
    order_events o
LEFT JOIN
    payment_events p
USING(order_id)
WHERE p.payment_date IS NOT NULL;