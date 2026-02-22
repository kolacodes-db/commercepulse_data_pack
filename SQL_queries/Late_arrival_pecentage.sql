CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.late_events` AS
SELECT
    COUNTIF(TIMESTAMP(event_date) < TIMESTAMP(created_at)) AS late_events,
    COUNT(*) AS total_events,
    ROUND(100 * COUNTIF(TIMESTAMP(event_date) < TIMESTAMP(created_at)) / COUNT(*), 2) AS late_event_percent
FROM
    `commercepulsedatapack.commercepulse.cleaned_data`;