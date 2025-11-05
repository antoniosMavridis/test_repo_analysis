CREATE OR REPLACE TABLE custom-manifest-113615.ad_hoc.EFBI_9715_pro_users as
WITH by_year AS (
  -- 2024-01-01 00:00:00  to  2024-09-04 00:00:00 (exclusive end)
  SELECT
    2024 AS yr,
    COUNT(DISTINCT deduplicated_user_id) AS total_users,
    COUNT(DISTINCT IF(is_subscriber, deduplicated_user_id, NULL)) AS pro_users
  FROM `custom-manifest-113615.analysis_datasets.analysis_core_from_tier1_orders`
  WHERE submit_dt >= TIMESTAMP('2024-01-01')
    AND submit_dt <  TIMESTAMP('2024-09-05')   -- inclusive up to 4th Sept
    AND chain_name IN ("Goody's Burger House", "Goody`s Burger House")
    AND vertical = 'Restaurant'
    AND brand = 'GR-EFOOD'
    AND is_accepted

  UNION ALL

  -- 2025-01-01 00:00:00  to  2025-09-05 00:00:00 (exclusive end)
  SELECT
    2025 AS yr,
    COUNT(DISTINCT deduplicated_user_id) AS total_users,
    COUNT(DISTINCT IF(is_subscriber, deduplicated_user_id, NULL)) AS pro_users
  FROM `custom-manifest-113615.analysis_datasets.analysis_core_from_tier1_orders`
  WHERE submit_dt >= TIMESTAMP('2025-01-01')
    AND submit_dt <  TIMESTAMP('2025-09-05')
    AND chain_name IN ("Goody's Burger House", "Goody`s Burger House")
    AND vertical = 'Restaurant'
    AND brand = 'GR-EFOOD'
    AND is_accepted
)
SELECT
  yr,
  total_users,
  pro_users,
  SAFE_DIVIDE(pro_users, total_users) AS pct_pro_users   -- % of users that are Pro
FROM by_year
ORDER BY yr;

