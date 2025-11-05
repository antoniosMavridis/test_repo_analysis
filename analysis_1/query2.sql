CREATE OR REPLACE TABLE custom-manifest-113615.ad_hoc.EFBI_9715_competition AS
WITH params AS (
  SELECT DATE '2024-01-01' AS start_2024,
         DATE '2024-09-04' AS end_2024,
         DATE '2025-01-01' AS start_2025,
         DATE '2025-09-04' AS end_2025,
         'Jan–Sep 4'        AS period_name
),
goody_basic_cuisines AS (
  SELECT DISTINCT basic_cuisine
  FROM `custom-manifest-113615.analysis_datasets.analysis_core_from_tier1_shops`
  WHERE chain_name IN ("Goody's Burger House", "Goody`s Burger House")
),
chains_in_scope AS (
  SELECT DISTINCT s.chain_name
  FROM `custom-manifest-113615.analysis_datasets.analysis_core_from_tier1_shops` s
  WHERE s.basic_cuisine IN (SELECT basic_cuisine FROM goody_basic_cuisines)
),
base AS (
  SELECT
      chain_name,
      EXTRACT(YEAR  FROM DATE(date))  AS yr,
      EXTRACT(MONTH FROM DATE(date))  AS mo,
      SUM(CASE WHEN status = TRUE THEN orderss ELSE 0 END)                     AS total_orders,
      SUM(COALESCE(affordability_orders,0))                                    AS sum_affordability_orders,
      SUM(CASE WHEN status = TRUE AND is_joker = TRUE THEN orderss ELSE 0 END) AS jokers
  FROM `custom-manifest-113615.analysis_datasets.ds_sources_core_from_tier1_optimized_daily_agg_per_shop`, params
  WHERE DATE(date) BETWEEN params.start_2024 AND params.end_2025
    AND EXTRACT(MONTH FROM DATE(date)) BETWEEN 1 AND 9   -- Jan–Sep
    AND brand   = 'GR-EFOOD'
    AND vertical = 'Restaurant'
    AND chain_name IN (SELECT chain_name FROM chains_in_scope)
  GROUP BY chain_name, yr, mo
),
base_2024 AS (
  SELECT
      b.chain_name, 2024 AS yr, b.mo,
      SUM(b.total_orders)             AS total_orders,
      SUM(b.sum_affordability_orders) AS sum_affordability_orders,
      SUM(b.jokers)                   AS jokers
  FROM base b, params p
  WHERE b.yr = 2024 AND DATE(2024, b.mo, 1) <= p.end_2024
  GROUP BY b.chain_name, yr, b.mo
),
base_2025 AS (
  SELECT
      b.chain_name, 2025 AS yr, b.mo,
      SUM(b.total_orders)             AS total_orders,
      SUM(b.sum_affordability_orders) AS sum_affordability_orders,
      SUM(b.jokers)                   AS jokers
  FROM base b, params p
  WHERE b.yr = 2025 AND DATE(2025, b.mo, 1) <= p.end_2025
  GROUP BY b.chain_name, yr, b.mo
),
base_clamped AS (
  SELECT * FROM base_2024
  UNION ALL
  SELECT * FROM base_2025
),
monthly AS (
  SELECT
      chain_name, yr, mo,
      total_orders,
      GREATEST(sum_affordability_orders - jokers, 0) AS offer_orders
  FROM base_clamped
),
period_totals AS (
  SELECT
      chain_name, yr, NULL AS mo,
      SUM(total_orders)                                        AS total_orders,
      GREATEST(SUM(sum_affordability_orders) - SUM(jokers),0)  AS offer_orders
  FROM base_clamped
  GROUP BY chain_name, yr
),
combined AS (
  SELECT * FROM monthly
  UNION ALL
  SELECT * FROM period_totals
),
pvt AS (
  SELECT
      c.chain_name,
      CASE
        WHEN c.mo IS NULL THEN (SELECT period_name FROM params)
        ELSE FORMAT('%04d-%02d', 2025, c.mo)
      END AS period_label,
      MAX(CASE WHEN yr = 2024 THEN total_orders END) AS orders_2024,
      MAX(CASE WHEN yr = 2025 THEN total_orders END) AS orders_2025,
      MAX(CASE WHEN yr = 2025 THEN offer_orders END) AS offer_orders_2025
  FROM combined c
  WHERE yr IN (2024, 2025)
  GROUP BY c.chain_name, period_label
),
with_total AS (
  SELECT * FROM pvt
  UNION ALL
  SELECT
      'Burgers (Total)' AS chain_name,
      period_label,
      SUM(orders_2024),
      SUM(orders_2025),
      SUM(offer_orders_2025)
  FROM pvt
  GROUP BY period_label
)
SELECT
    chain_name                                        AS Shop,
    period_label                                      AS Month_or_Total,     -- 2025-01..2025-09, Jan–Sep 4
    orders_2024,
    orders_2025,
    SAFE_DIVIDE(orders_2025 - orders_2024, NULLIF(orders_2024, 0)) AS yoy_orders_pct,
    SAFE_DIVIDE(offer_orders_2025, NULLIF(orders_2025, 0))         AS offer_orders_share_2025_pct
FROM with_total, params
WHERE period_label IN (
  (SELECT period_name FROM params),
  '2025-01','2025-02','2025-03','2025-04','2025-05','2025-06','2025-07','2025-08','2025-09'
)
ORDER BY
  CASE WHEN chain_name = 'Burgers (Total)' THEN 0 ELSE 1 END,
  chain_name,
  CASE period_label WHEN (SELECT period_name FROM params) THEN 0 ELSE 1 END,
  period_label;
