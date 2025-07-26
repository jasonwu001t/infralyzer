-- AWS Glue Daily Cost and Usage Analysis
-- Description: Daily unblended cost and usage information with resource ID extraction for jobs and crawlers
-- Output: Account details, operation type, resource ID, and costs sorted by date and usage
-- Source: AWS CUR Query Library - Analytics section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  DATE_FORMAT((line_item_usage_start_date),'%Y-%m-%d') AS day_line_item_usage_start_date,
  line_item_operation,
  CASE
    WHEN LOWER(line_item_operation) = 'jobrun' THEN SPLIT_PART(line_item_resource_id, 'job/', 2)
    WHEN LOWER(line_item_operation) = 'crawlerrun' THEN SPLIT_PART(line_item_resource_id, 'crawler/', 2)
    ELSE 'N/A'
  END AS case_line_item_resource_id,
  SUM(CAST(line_item_usage_amount AS DOUBLE)) AS sum_line_item_usage_amount,
  SUM(CAST(line_item_unblended_cost AS DECIMAL(16, 8))) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  -- ${date_filter}
  product['product_name'] = ('AWS Glue')
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  DATE_FORMAT((line_item_usage_start_date),'%Y-%m-%d'),
  line_item_operation,
  line_item_resource_id
ORDER BY
  day_line_item_usage_start_date,
  sum_line_item_usage_amount,
  sum_line_item_unblended_cost;