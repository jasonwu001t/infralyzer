-- Amazon Kinesis Daily Cost and Usage Analysis
-- Description: Daily unblended cost and usage information for all Kinesis products (Data Streams, Firehose, Analytics, Video Streams)
-- Output: Account details, resource ID, product name, and costs in descending order
-- Source: AWS CUR Query Library - Analytics section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date,
  line_item_resource_id AS split_line_item_resource_id,
  product_product_name AS product_product_name,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  line_item_product_code LIKE '%Kinesis%'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  line_item_resource_id,
  product_product_name
ORDER BY
  day_line_item_usage_start_date,
  sum_line_item_unblended_cost DESC 