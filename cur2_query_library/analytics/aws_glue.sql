-- AWS Glue Daily Cost and Usage Analysis
-- Description: Daily unblended cost and usage information with resource ID extraction for jobs and crawlers
-- Output: Account details, operation type, resource ID, and costs sorted by date and usage
-- Source: AWS CUR Query Library - Analytics section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date,
  line_item_operation,
  CASE
    WHEN LOWER(line_item_operation) = 'jobrun' THEN line_item_resource_id
    WHEN LOWER(line_item_operation) = 'crawlerrun' THEN line_item_resource_id
    ELSE 'N/A'
  END AS case_line_item_resource_id,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  product_product_name = 'AWS Glue'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  line_item_operation,
  line_item_resource_id
ORDER BY
  day_line_item_usage_start_date,
  sum_line_item_usage_amount,
  sum_line_item_unblended_cost 