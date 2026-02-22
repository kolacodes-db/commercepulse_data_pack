CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.dim_customer` AS
SELECT
    customer_id,
    buyer_email AS customer_email,
    buyer_phone AS customer_phone,
    state AS region,
    TRIM(CONCAT(
        COALESCE(state, ''), 
        IF(state IS NOT NULL AND city IS NOT NULL, ', ', ''), 
        COALESCE(city, ''), 
        IF((state IS NOT NULL OR city IS NOT NULL) AND country IS NOT NULL, ', ', ''), 
        COALESCE(country, '')
    )) AS customer_address
FROM `commercepulsedatapack.commercepulse.cleaned_data`
GROUP BY customer_id, buyer_email, buyer_phone, state, city, country;