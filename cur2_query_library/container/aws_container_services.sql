-- AWS Container Services Daily Usage and Cost Analysis
-- Description: Daily usage, unblended, and amortized cost per service, operation, and resource ID for ECS/EKS
-- Output: Daily cost and usage by account, resource, service, and operation
-- Source: AWS CUR Query Library - Container section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  SPLIT_PART(line_item_resource_id, ':', 6) AS split_line_item_resource_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  line_item_operation,
  line_item_product_code,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost,
  SUM(CASE
    WHEN line_item_line_item_type = 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost
    WHEN line_item_line_item_type = 'SavingsPlanRecurringFee' THEN (savings_plan_total_commitment_to_date - savings_plan_used_commitment)
    WHEN line_item_line_item_type = 'SavingsPlanNegation' THEN 0
    WHEN line_item_line_item_type = 'SavingsPlanUpfrontFee' THEN 0
    WHEN line_item_line_item_type = 'DiscountedUsage' THEN reservation_effective_cost
    ELSE line_item_unblended_cost 
  END) AS sum_amortized_cost
FROM 
  CUR 
WHERE 
  line_item_product_code IN ('AmazonECS','AmazonEKS')
  AND line_item_line_item_type  IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage','SavingsPlanRecurringFee','SavingsPlanNegation','SavingsPlanUpfrontFee')
GROUP BY 
  bill_payer_account_id,
  line_item_usage_account_id,
  SPLIT_PART(line_item_resource_id, ':', 6),
  line_item_usage_start_date::date,
  line_item_operation,
  line_item_product_code
ORDER BY 
  day_line_item_usage_start_date ASC,
  sum_line_item_unblended_cost DESC,
  sum_line_item_usage_amount DESC,
  line_item_operation 