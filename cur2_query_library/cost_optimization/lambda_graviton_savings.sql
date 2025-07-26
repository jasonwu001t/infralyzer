-- Lambda Graviton Cost Savings Analysis
-- Description: Lambda functions by processor architecture, with potential ARM64 savings
-- Output: Lambda resource, architecture, unblended cost, and potential ARM savings
-- Source: AWS CUR Query Library - Cost Optimization section
-- Note: Simplified for Polars compatibility

SELECT
  line_item_resource_id,
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_line_item_type,
  CASE
    WHEN RIGHT(line_item_usage_type, 3) = 'ARM' THEN 'arm64'
    ELSE 'x86_64'
  END AS processor,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost,
  SUM(CASE WHEN RIGHT(line_item_usage_type, 3) = 'ARM' THEN 0 ELSE line_item_unblended_cost * 0.2 END) AS potential_arm_savings
FROM
  CUR
WHERE
  line_item_product_code = 'AWSLambda'
  AND line_item_operation = 'Invoke'
  AND (line_item_usage_type LIKE '%Request%' OR line_item_usage_type LIKE '%Lambda-GB-Second%')
  AND line_item_usage_start_date > CURRENT_DATE - INTERVAL '1 month'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY
  line_item_resource_id,
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_line_item_type,
  processor
ORDER BY
  sum_line_item_unblended_cost DESC 