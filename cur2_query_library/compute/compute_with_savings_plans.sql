-- Compute with Savings Plans Analysis
-- Description: Compute usage covered by Savings Plans with cost comparison to On-Demand pricing
-- Output: Savings Plans details, usage amounts, and effective cost savings
-- Source: AWS CUR Query Library - Compute section

SELECT 
  bill_payer_account_id,
  bill_billing_period_start_date,
  line_item_usage_account_id,
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') AS month_line_item_usage_start_date, 
  savings_plan_savings_plan_a_r_n,
  line_item_product_code,
  line_item_usage_type,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  line_item_line_item_description,
  pricing_public_on_demand_rate,
  SUM(pricing_public_on_demand_cost) AS sum_pricing_public_on_demand_cost,
  savings_plan_savings_plan_rate,
  SUM(savings_plan_savings_plan_effective_cost) AS sum_savings_plan_savings_plan_effective_cost
FROM 
  CUR 
WHERE 
  line_item_line_item_type LIKE 'SavingsPlanCoveredUsage'
GROUP BY 
  bill_payer_account_id, 
  bill_billing_period_start_date, 
  line_item_usage_account_id, 
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0'), 
  savings_plan_savings_plan_a_r_n, 
  line_item_product_code, 
  line_item_usage_type, 
  line_item_unblended_rate, 
  line_item_line_item_description, 
  pricing_public_on_demand_rate, 
  savings_plan_savings_plan_rate
ORDER BY 
  sum_pricing_public_on_demand_cost DESC 