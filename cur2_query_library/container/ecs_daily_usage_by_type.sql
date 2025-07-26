-- Amazon ECS Daily Usage Hours and Cost by Usage Type and Purchase Option
-- Description: Daily ECS cost and usage per resource, by usage type and purchase option
-- Output: Daily cost and usage by resource, usage type, and purchase option
-- Source: AWS CUR Query Library - Container section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  SPLIT_PART(SPLIT_PART(line_item_resource_id, ':', 6), '/', 2) AS split_line_item_resource_id,
  CASE
    WHEN line_item_usage_type LIKE '%Fargate-GB%' THEN 'GB per hour'
    WHEN line_item_usage_type LIKE '%Fargate-vCPU%' THEN 'vCPU per hour'
    ELSE NULL
  END AS case_line_item_usage_type,
  CASE
    WHEN line_item_line_item_type IN ('SavingsPlanCoveredUsage', 'SavingsPlanNegation') THEN savings_plan_offering_type
    ELSE 
      CASE pricing_term
        WHEN 'OnDemand' THEN 'OnDemand'
        WHEN '' THEN 'Spot Instance'
        ELSE pricing_term
      END
  END AS case_purchase_option,
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
  line_item_product_code = 'AmazonECS'
  AND line_item_operation != 'ECSTask-EC2'
  AND product_product_family != 'Data Transfer'
  AND line_item_line_item_type  IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage','SavingsPlanNegation','SavingsPlanRecurringFee','SavingsPlanUpfrontFee')
GROUP BY 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  SPLIT_PART(SPLIT_PART(line_item_resource_id, ':', 6), '/', 2),
  CASE
    WHEN line_item_usage_type LIKE '%Fargate-GB%' THEN 'GB per hour'
    WHEN line_item_usage_type LIKE '%Fargate-vCPU%' THEN 'vCPU per hour'
    ELSE NULL
  END,
  CASE
    WHEN line_item_line_item_type IN ('SavingsPlanCoveredUsage', 'SavingsPlanNegation') THEN savings_plan_offering_type
    ELSE 
      CASE pricing_term
        WHEN 'OnDemand' THEN 'OnDemand'
        WHEN '' THEN 'Spot Instance'
        ELSE pricing_term
      END
  END
ORDER BY 
  day_line_item_usage_start_date ASC,
  case_purchase_option,
  sum_line_item_usage_amount DESC 