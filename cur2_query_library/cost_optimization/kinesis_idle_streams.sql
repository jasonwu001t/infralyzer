-- Amazon Kinesis Data Streams - Idle Streams Analysis
-- Description: Cost and usage of Kinesis Data Streams with no data puts/gets in the last month
-- Output: Streams with no activity, cost, and usage for potential deletion
-- Source: AWS CUR Query Library - Cost Optimization section
-- Note: Window functions are not supported in Polars SQL, so this is a simplified version

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_resource_id,
  product_region,
  pricing_unit,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  line_item_product_code = 'AmazonKinesis'
  AND line_item_usage_type <> 'OnDemand-BilledOutgoingEFOBytes'
  AND line_item_usage_type NOT LIKE '%Extended-ShardHour%'
  AND line_item_usage_start_date >= (CURRENT_DATE - INTERVAL '30 days')
  AND line_item_line_item_type = 'Usage'
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_resource_id,
  product_region,
  pricing_unit
ORDER BY
  sum_line_item_unblended_cost DESC 