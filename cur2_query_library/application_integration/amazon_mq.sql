-- Amazon MQ Daily Cost and Usage Analysis
-- Description: Daily unblended and amortized cost with usage information per linked account for Amazon MQ
-- Output: Account details, broker engine, resource ID, usage type, and costs in descending order
-- Source: AWS CUR Query Library - Application Integration section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  product_broker_engine AS product_broker_engine,
  line_item_usage_type,
  product_product_family,
  pricing_unit,
  pricing_term,
  line_item_usage_type AS split_line_item_usage_type,
  line_item_resource_id AS split_line_item_resource_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  line_item_operation,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM 
  CUR
WHERE 
  product_product_name = 'Amazon MQ'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY 
  bill_payer_account_id,
  line_item_usage_account_id,
  product_broker_engine,
  product_product_family,
  pricing_unit,
  pricing_term,
  line_item_usage_start_date::date, 
  line_item_usage_type,
  line_item_resource_id,
  line_item_operation
ORDER BY 
  day_line_item_usage_start_date,
  sum_line_item_unblended_cost DESC,
  split_line_item_usage_type 