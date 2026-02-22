
CREATE OR REPLACE TABLE `commercepulsedatapack.commercepulse.dim_product` AS
SELECT
    sku AS product_id,
    vendor,
    MAX(unit_price) AS unit_price,
    MAX(unit_price_ngn) AS unit_price_ngn
FROM `commercepulsedatapack.commercepulse.cleaned_data`
GROUP BY sku, vendor;