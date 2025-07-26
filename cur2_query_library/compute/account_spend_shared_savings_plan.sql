-- Account Spend of Shared Savings Plan Analysis
-- Description: Accounts utilizing AWS Savings Plans for which they are not the buyer
-- Output: Usage accounts, savings plan owners, and effective cost savings
-- Source: AWS CUR Query Library - Compute section
-- Note: Update account IDs (111122223333, 444455556666) as needed for your use case

SELECT 
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') AS month_line_item_usage_start_date,
  bill_payer_account_id,
  line_item_usage_account_id,
  SPLIT_PART(savings_plan_savings_plan_a_r_n, ':', 5) AS savings_plan_owner_account_id,
  savings_plan_offering_type,
  line_item_resource_id,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost,
  SUM(savings_plan_savings_plan_effective_cost) AS sum_savings_plan_savings_plan_effective_cost
FROM 
  CUR 
WHERE 
  bill_payer_account_id = '111122223333' 
  AND line_item_usage_account_id = '444455556666' 
  AND line_item_line_item_type = 'SavingsPlanCoveredUsage'
  AND savings_plan_savings_plan_a_r_n NOT LIKE '%444455556666%'
GROUP BY 
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0'),
  line_item_resource_id,
  line_item_usage_account_id,
  SPLIT_PART(savings_plan_savings_plan_a_r_n, ':', 5),
  bill_payer_account_id,
  savings_plan_offering_type
ORDER BY 
  sum_savings_plan_savings_plan_effective_cost DESC 