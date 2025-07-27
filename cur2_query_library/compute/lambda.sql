-- EC2 Cost and Usage Analysis
-- Description: EC2 cost breakdown by different usage elements with simplified analysis
-- Output: Resource details, usage categorization, and costs
-- Source: AWS CUR Query Library - Compute section
-- Note: Updated for available dataset (EC2 instead of Lambda)

SELECT
  bill_payer_account_id,
  line_item_usage_account_id, 
  line_item_line_item_type,
  line_item_usage_start_date::date AS day_line_item_usage_start_date,
  product_region_code AS region,
  CASE
    WHEN line_item_usage_type LIKE '%BoxUsage%' THEN 'EC2 Instance Usage'
    WHEN line_item_usage_type LIKE '%EBS%' THEN 'EBS Storage'
    WHEN line_item_usage_type LIKE '%DataTransfer%' THEN 'Data Transfer'
    WHEN line_item_usage_type LIKE '%NatGateway%' THEN 'NAT Gateway'
    ELSE 'Other'
  END AS usage_category,
  line_item_resource_id,
  product_instance_type,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM 
  CUR
WHERE 
  line_item_product_code = 'AmazonEC2'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
  AND line_item_unblended_cost > 0
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_line_item_type,
  line_item_usage_start_date::date,
  product_region_code,
  CASE
    WHEN line_item_usage_type LIKE '%BoxUsage%' THEN 'EC2 Instance Usage'
    WHEN line_item_usage_type LIKE '%EBS%' THEN 'EBS Storage'
    WHEN line_item_usage_type LIKE '%DataTransfer%' THEN 'Data Transfer'
    WHEN line_item_usage_type LIKE '%NatGateway%' THEN 'NAT Gateway'
    ELSE 'Other'
  END,
  line_item_resource_id,
  product_instance_type
ORDER BY
  sum_line_item_unblended_cost DESC,
  day_line_item_usage_start_date

LIMIT 20;