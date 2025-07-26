-- Amazon SQS Top 20 Daily Unblended Costs Analysis
-- Description: Top 20 daily unblended costs and usage information for Amazon SQS with resource details
-- Output: Account details, resource ID (queue), usage type, operation, and costs in descending order
-- Source: AWS CUR Query Library - Application Integration section

SELECT 
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  line_item_usage_type,
  line_item_operation,
  line_item_resource_id,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM 
  CUR 
WHERE 
  line_item_product_code = 'AWSQueueService'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY 
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  line_item_usage_type,
  line_item_operation,
  line_item_resource_id
ORDER BY 
  sum_line_item_unblended_cost DESC
LIMIT 20 