-- AWS Outposts - EC2 Hours a Day Analysis
-- Description: Amazon EC2 software costs on AWS Outposts including Windows, RHEL, and other OS
-- Output: Instance details, operating system costs, and usage quantities for Outposts
-- Source: AWS CUR Query Library - Compute section

SELECT 
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  bill_payer_account_id, 
  line_item_usage_account_id,  
  line_item_resource_id,
  product_instance_type,
  product_operating_system,
  product_pre_installed_sw,
  line_item_line_item_description, 
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(CASE
    WHEN line_item_line_item_type = 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost
    WHEN line_item_line_item_type = 'DiscountedUsage' THEN reservation_effective_cost
    WHEN line_item_line_item_type = 'Usage' THEN line_item_unblended_cost
    ELSE 0 
  END) AS sum_amortized_cost
FROM 
  CUR 
WHERE 
  product_location_type = 'AWS Outposts'
  AND product_product_family = 'Compute Instance'
  AND line_item_operation LIKE '%RunInstance%'
  AND line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage')
GROUP BY
  line_item_usage_start_date::date,
  bill_payer_account_id, 
  line_item_usage_account_id,  
  line_item_resource_id,
  product_instance_type,
  product_operating_system,
  product_pre_installed_sw,
  line_item_line_item_description
ORDER BY
  day_line_item_usage_start_date ASC,
  sum_amortized_cost DESC 