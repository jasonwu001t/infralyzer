-- EC2 Instance Cost by Pricing Model Analysis
-- Description: Group instance usage by account and pricing model for cost optimization
-- Output: Instance type, pricing model, and cost by account
-- Source: AWS CUR Query Library - Cost Optimization section
-- Note: Simplified for Polars compatibility

SELECT
  line_item_usage_account_id,
  CASE 
    WHEN reservation_reservation_a_r_n <> '' THEN SPLIT_PART(reservation_reservation_a_r_n, ':', 5)
    WHEN savings_plan_savings_plan_a_r_n <> '' THEN SPLIT_PART(savings_plan_savings_plan_a_r_n, ':', 5)
    ELSE 'NA'
  END AS ri_sp_owner_id,
  CASE 
    WHEN line_item_usage_type LIKE '%SpotUsage%' THEN 'Spot'
    WHEN (product_usagetype LIKE '%BoxUsage%' OR product_usagetype LIKE '%DedicatedUsage:%') AND line_item_line_item_type = 'SavingsPlanCoveredUsage' THEN 'SavingsPlan'
    WHEN (product_usagetype LIKE '%BoxUsage%' AND line_item_line_item_type = 'DiscountedUsage') THEN 'ReservedInstance'
    WHEN ((product_usagetype LIKE '%BoxUsage%' OR product_usagetype LIKE '%DedicatedUsage:%') AND line_item_line_item_type = 'Usage') THEN 'OnDemand'
    ELSE 'Other' END AS pricing_model,
  CASE 
    WHEN line_item_usage_type LIKE '%BoxUsage%' OR line_item_usage_type LIKE '%DedicatedUsage%' THEN product_instance_type
    ELSE SPLIT_PART(line_item_usage_type, ':', 2) END AS instance_type,
  ROUND(SUM(line_item_unblended_cost), 2) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  line_item_operation LIKE '%RunInstance%'
  AND line_item_product_code = 'AmazonEC2'
  AND (product_instance_type <> '' OR (line_item_usage_type LIKE '%SpotUsage%' AND line_item_line_item_type = 'Usage'))
GROUP BY
  line_item_usage_account_id,
  ri_sp_owner_id,
  pricing_model,
  instance_type
ORDER BY
  pricing_model,
  sum_line_item_unblended_cost DESC 