CREATE OR REPLACE TABLE custom-manifest-113615.ad_hoc.EFBI_9715_competition_subperiods AS
WITH goody_basic_cuisines AS (
  SELECT DISTINCT basic_cuisine
  FROM `custom-manifest-113615.analysis_datasets.analysis_core_from_tier1_shops`
  WHERE chain_name IN ("Goody's Burger House", "Goody`s Burger House")
),
chains_in_scope AS (
  SELECT DISTINCT s.chain_name
  FROM `custom-manifest-113615.analysis_datasets.analysis_core_from_tier1_shops` s
  WHERE s.basic_cuisine IN (SELECT basic_cuisine FROM goody_basic_cuisines)
),
src AS (
  SELECT
      chain_name,
      DATE(date) AS d,
      EXTRACT(YEAR FROM DATE(date)) AS yr,
      CASE
        WHEN (DATE(date) BETWEEN '2024-01-01' AND '2024-01-20')
          OR (DATE(date) BETWEEN '2025-01-01' AND '2025-01-20')
          THEN 'P1: Jan 01–Jan 20 (0 €)'
        WHEN (DATE(date) BETWEEN '2024-01-21' AND '2024-04-28')
          OR (DATE(date) BETWEEN '2025-01-21' AND '2025-04-28')
          THEN 'P2: Jan 21–Apr 28 (0.5 €)'
        WHEN (DATE(date) BETWEEN '2024-04-29' AND '2024-05-31')
          OR (DATE(date) BETWEEN '2025-04-29' AND '2025-05-31')
          THEN 'P3: Apr 29–May 31 (1 €)'
      END AS period_label,
      CASE WHEN status = TRUE THEN orderss ELSE 0 END AS total_orders_d,
      affordability_orders AS affordability_orders_d,
      CASE WHEN status = TRUE AND is_joker = TRUE THEN orderss ELSE 0 END AS jokers_d
  FROM `custom-manifest-113615.analysis_datasets.ds_sources_core_from_tier1_optimized_daily_agg_per_shop`
  WHERE DATE(date) BETWEEN '2024-01-01' AND '2025-05-31'
    AND brand   = 'GR-EFOOD'
    AND vertical = 'Restaurant'
    AND chain_name IN (SELECT chain_name FROM chains_in_scope)
),
agg AS (
  SELECT
      chain_name,
      yr,
      period_label,
      SUM(total_orders_d) AS total_orders,
      GREATEST(SUM(affordability_orders_d) - SUM(jokers_d), 0) AS offer_orders
  FROM src
  WHERE period_label IS NOT NULL
  GROUP BY chain_name, yr, period_label
),
pvt AS (
  SELECT
      chain_name,
      period_label,
      MAX(CASE WHEN yr = 2024 THEN total_orders END) AS orders_2024,
      MAX(CASE WHEN yr = 2025 THEN total_orders END) AS orders_2025,
      MAX(CASE WHEN yr = 2025 THEN offer_orders END) AS offer_orders_2025
  FROM agg
  WHERE yr IN (2024, 2025)
  GROUP BY chain_name, period_label
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
    period_label                                      AS Subperiod,
    orders_2024,
    orders_2025,
    100 * SAFE_DIVIDE(orders_2025 - orders_2024, NULLIF(orders_2024, 0)) AS yoy_orders_pct,
    100 * SAFE_DIVIDE(offer_orders_2025, NULLIF(orders_2025, 0))         AS offer_orders_share_2025_pct
FROM with_total
WHERE period_label IN (
  'P1: Jan 01–Jan 20 (0 €)',
  'P2: Jan 21–Apr 28 (0.5 €)',
  'P3: Apr 29–May 31 (1 €)'
)
ORDER BY
  CASE WHEN chain_name = 'Burgers (Total)' THEN 0 ELSE 1 END,
  chain_name,
  CASE period_label
    WHEN 'P1: Jan 01–Jan 20 (0 €)' THEN 1
    WHEN 'P2: Jan 21–Apr 28 (0.5 €)' THEN 2
    WHEN 'P3: Apr 29–May 31 (1 €)' THEN 3
    ELSE 99
  END;
