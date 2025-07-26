-- Graviton Usage Analysis
-- Description: Graviton-based usage, amortized cost, usage hours, and unique resource count
-- Output: Daily Graviton usage and cost by account, service, instance type, and region
-- Source: AWS CUR Query Library - Cost Optimization section
-- Note: Simplified for Polars compatibility

SELECT 
  line_item_usage_start_date::date AS day_line_item_usage_start_date,
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_product_code,
  product_instance_type,
  product_region,
  SUM(CASE
    WHEN line_item_line_item_type = 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost
    WHEN line_item_line_item_type = 'DiscountedUsage' THEN reservation_effective_cost
    WHEN line_item_line_item_type = 'Usage' THEN line_item_unblended_cost
    ELSE 0 
  END) AS sum_amortized_cost, 
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount, 
  COUNT(DISTINCT line_item_resource_id) AS count_line_item_resource_id
FROM 
  CUR
WHERE 
  line_item_usage_type LIKE '%g.%' 
  AND line_item_usage_type NOT LIKE '%EBSOptimized%' 
  AND line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage')
GROUP BY
  line_item_usage_start_date::date,
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_product_code,
  product_instance_type,
  product_region
ORDER BY 
  day_line_item_usage_start_date DESC,
  sum_amortized_cost DESC 