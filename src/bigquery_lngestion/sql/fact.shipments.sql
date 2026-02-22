CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.fact_shipments` AS
SELECT
    event_id,
    order_id,
    DATE(event_date) AS shipment_date,
    `event_type`,
    CASE
        WHEN `event_type` = 'shipment_updated' OR `event_type` = 'historical_order' THEN 'Order Shipped'
        ELSE 'Order Not Shipped'
    END AS shipment_status
FROM `commercepulsedatapack.commercepulse.cleaned_data`;