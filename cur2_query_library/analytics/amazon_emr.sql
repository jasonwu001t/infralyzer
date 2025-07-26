-- Amazon EMR Daily Cost and Usage Analysis
-- Description: Daily unblended cost and usage information per linked account for Amazon EMR
-- Output: Account details, usage type, and costs in descending order
-- Source: AWS CUR Query Library - Analytics section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date,
  line_item_usage_type AS split_line_item_usage_type,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  product_product_name = 'Amazon Elastic MapReduce'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  line_item_usage_type,
  line_item_line_item_type
ORDER BY
  day_line_item_usage_start_date,
  sum_line_item_usage_amount,
  sum_line_item_unblended_cost DESC,
  split_line_item_usage_type 