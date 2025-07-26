-- Amazon Athena Daily Cost and Usage Analysis
-- Description: Daily unblended cost and usage information for Amazon Athena with resource details
-- Output: Account, usage details, resource ID, region, and costs in descending order
-- Source: AWS CUR Query Library - Analytics section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  line_item_usage_type,
  line_item_resource_id,
  product['region'],
  line_item_product_code,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM 
  CUR 
WHERE 
  line_item_product_code = 'AmazonAthena'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  line_item_usage_type,
  line_item_resource_id,
  product['region'],
  line_item_product_code
ORDER BY 
  sum_line_item_unblended_cost DESC
LIMIT 20 