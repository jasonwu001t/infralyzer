-- Elastic Load Balancing - Idle ELB Analysis
-- Description: Cost and usage of ELBs with no traffic last month and ran for more than 336 hours
-- Output: Idle ELBs, cost, and usage for potential deletion
-- Source: AWS CUR Query Library - Cost Optimization section
-- Note: Window functions are not supported in Polars SQL, so this is a simplified version

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  SPLIT_PART(line_item_resource_id, ':', 6) AS split_line_item_resource_id,
  product_region,
  pricing_unit,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  line_item_product_code = 'AWSELB'
  AND line_item_usage_start_date >= (CURRENT_DATE - INTERVAL '1 month')
  AND line_item_line_item_type = 'Usage'
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  SPLIT_PART(line_item_resource_id, ':', 6),
  product_region,
  pricing_unit
HAVING
  SUM(line_item_usage_amount) > 336
ORDER BY
  sum_line_item_unblended_cost DESC 