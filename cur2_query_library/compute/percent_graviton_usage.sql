-- Percent of Graviton Usage Analysis
-- Description: Graviton-based usage percentage across payer and linked accounts
-- Output: Account details, service usage, and graviton percentage calculations
-- Source: AWS CUR Query Library - Compute section
-- Note: Simplified for Polars compatibility - regex patterns may need adjustment

WITH graviton_usage AS (
  SELECT 
    EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
    LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') AS month_line_item_usage_start_date,
    bill_payer_account_id,
    line_item_usage_account_id,
    line_item_product_code,
    SUM(line_item_usage_amount) AS sum_grav_usage_amount
  FROM CUR
  WHERE 
    line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
    AND (
      -- Graviton instances (simplified pattern)
      line_item_usage_type LIKE '%g.%' 
      OR line_item_usage_type LIKE '%6g%' 
      OR line_item_usage_type LIKE '%7g%'
      OR line_item_usage_type LIKE '%ARM%'
    )
  GROUP BY 
    EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
    LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0'),
    bill_payer_account_id,
    line_item_usage_account_id,
    line_item_product_code
),
total_usage AS (
  SELECT 
    EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
    LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') AS month_line_item_usage_start_date,
    bill_payer_account_id,
    line_item_usage_account_id,
    line_item_product_code,
    SUM(line_item_usage_amount) AS sum_line_item_usage_amount
  FROM CUR
  WHERE 
    line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
    AND bill_billing_entity NOT LIKE '%Marketplace%'
    AND (
      -- All compute instances
      line_item_usage_type LIKE '%:%'
      OR line_item_product_code IN ('AmazonECS', 'ElasticMapReduce', 'AWSLambda')
    )
  GROUP BY 
    EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
    LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0'),
    bill_payer_account_id,
    line_item_usage_account_id,
    line_item_product_code
)

SELECT 
  t.month_line_item_usage_start_date,
  t.bill_payer_account_id,
  t.line_item_usage_account_id,
  t.line_item_product_code,
  COALESCE(g.sum_grav_usage_amount, 0) AS sum_grav_usage_amount,
  t.sum_line_item_usage_amount,
  CASE 
    WHEN t.sum_line_item_usage_amount > 0 THEN 
      ROUND(COALESCE(g.sum_grav_usage_amount, 0) / t.sum_line_item_usage_amount, 2)
    ELSE 0
  END AS percent_graviton_usage
FROM total_usage t
LEFT JOIN graviton_usage g 
  ON t.line_item_usage_account_id = g.line_item_usage_account_id
  AND t.line_item_product_code = g.line_item_product_code
  AND t.month_line_item_usage_start_date = g.month_line_item_usage_start_date
WHERE COALESCE(g.sum_grav_usage_amount, 0) > 0
ORDER BY 
  t.month_line_item_usage_start_date,
  percent_graviton_usage DESC 