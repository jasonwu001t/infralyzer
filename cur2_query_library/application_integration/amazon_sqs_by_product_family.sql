-- Amazon SQS By Product Family Daily Cost Analysis
-- Description: Daily unblended cost and usage information for Amazon SQS grouped by account and operation
-- Output: Account details, product family with operation concatenation, and costs ordered by date and cost
-- Source: AWS CUR Query Library - Application Integration section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date,
  product_product_family || ' - ' || line_item_operation AS concat_product_product_family,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  line_item_product_code = 'AWSQueueService'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY
  bill_payer_account_id, 
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  product_product_family || ' - ' || line_item_operation
ORDER BY
  day_line_item_usage_start_date,
  sum_line_item_unblended_cost DESC 