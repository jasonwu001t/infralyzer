-- EC2 Effective Savings Plans Analysis
-- Description: EC2 consumption of Savings Plans across Compute resources by linked accounts
-- Output: Savings Plans details, consumption, and effective savings by account and plan
-- Source: AWS CUR Query Library - Compute section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  SPLIT_PART(savings_plan_savings_plan_a_r_n, '/', 2) AS savings_plan_savings_plan_a_r_n,
  savings_plan_offering_type,
  savings_plan_region,
  CASE 
    WHEN line_item_product_code = 'AmazonECS' THEN 'Fargate'
    WHEN line_item_product_code = 'AWSLambda' THEN 'Lambda'
    ELSE product_instance_type_family 
  END AS case_instance_type_family,
  savings_plan_end_time,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost,
  SUM(savings_plan_savings_plan_effective_cost) AS sum_savings_plan_savings_plan_effective_cost,
  SUM(line_item_unblended_cost) - SUM(savings_plan_savings_plan_effective_cost) AS sum_savings_plan_effective_savings_amount
FROM 
  CUR 
WHERE 
  savings_plan_savings_plan_a_r_n <> ''
  AND savings_plan_savings_plan_a_r_n IS NOT NULL
  AND line_item_line_item_type = 'SavingsPlanCoveredUsage'
GROUP BY 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  SPLIT_PART(savings_plan_savings_plan_a_r_n, '/', 2),
  savings_plan_offering_type,
  savings_plan_region,
  CASE 
    WHEN line_item_product_code = 'AmazonECS' THEN 'Fargate'
    WHEN line_item_product_code = 'AWSLambda' THEN 'Lambda'
    ELSE product_instance_type_family 
  END,
  savings_plan_end_time
ORDER BY 
  day_line_item_usage_start_date 