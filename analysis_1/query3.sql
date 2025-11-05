CREATE OR REPLACE TABLE
  `custom-manifest-113615.ad_hoc.EFBI_9715_daily_df` 
AS
SELECT  
    DATE(submit_dt) AS date,
    CAST(delivery_cost AS STRING) AS delivery_cost,
    COUNT(DISTINCT order_id) AS orders
FROM `custom-manifest-113615.analysis_datasets.analysis_core_from_tier1_orders` 
WHERE submit_dt BETWEEN '2024-01-01' AND '2025-05-31'
  AND chain_name IN ("Goody's Burger House", "Goody`s Burger House")
  AND brand = 'GR-EFOOD'
  AND is_accepted
  AND vertical = 'Restaurant'
GROUP BY DATE(submit_dt), CAST(delivery_cost AS STRING)
ORDER BY date, delivery_cost;



-- Check
-- SELECT 
--     delivery_cost,
--     MIN(DATE(submit_dt)) AS first_date,
--     MAX(DATE(submit_dt)) AS last_date
-- FROM `custom-manifest-113615.analysis_datasets.analysis_core_from_tier1_orders`
-- WHERE submit_dt BETWEEN '2024-01-01' AND '2025-05-31'
--   AND chain_name IN ("Goody's Burger House", "Goody`s Burger House")
--   AND brand = 'GR-EFOOD'
--   AND is_accepted
--   AND vertical = 'Restaurant'
-- GROUP BY delivery_cost
-- ORDER BY delivery_cost;
