-- Amazon OpenSearch Service Daily Cost and Usage Analysis
-- Description: Daily unblended and amortized cost with usage information including RI and SP calculations
-- Output: Account details, resource ID, instance details, pricing terms, and comprehensive cost breakdown
-- Source: AWS CUR Query Library - Analytics section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date,
  line_item_resource_id AS split_line_item_resource_id,
  product_product_family AS product_product_family,
  product_instance_family AS product_instance_family,
  product_instance_type AS product_instance_type,
  pricing_term,
  product_storage_media AS product_storage_media,
  product_transfer_type AS product_transfer_type,
  
  -- Usage amount calculation
  SUM(CASE 
    WHEN (line_item_line_item_type = 'SavingsPlanCoveredUsage') THEN line_item_usage_amount 
    WHEN (line_item_line_item_type = 'DiscountedUsage') THEN line_item_usage_amount 
    WHEN (line_item_line_item_type = 'Usage') THEN line_item_usage_amount 
    ELSE 0 
  END) AS sum_line_item_usage_amount,
  
  -- Unblended cost
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost,
  
  -- Amortized cost calculation
  SUM(CASE
    WHEN (line_item_line_item_type = 'SavingsPlanCoveredUsage') THEN savings_plan_savings_plan_effective_cost 
    WHEN (line_item_line_item_type = 'SavingsPlanRecurringFee') THEN (savings_plan_total_commitment_to_date - savings_plan_used_commitment) 
    WHEN (line_item_line_item_type = 'SavingsPlanNegation') THEN 0
    WHEN (line_item_line_item_type = 'SavingsPlanUpfrontFee') THEN 0
    WHEN (line_item_line_item_type = 'DiscountedUsage') THEN reservation_effective_cost  
    WHEN (line_item_line_item_type = 'RIFee') THEN (reservation_unused_amortized_upfront_fee_for_billing_period + reservation_unused_recurring_fee)
    WHEN ((line_item_line_item_type = 'Fee') AND (reservation_reservation_a_r_n != '')) THEN 0 
    ELSE line_item_unblended_cost 
  END) AS sum_amortized_cost,
  
  -- RI/SP true-up fees
  SUM(CASE
    WHEN (line_item_line_item_type = 'SavingsPlanRecurringFee') THEN (-savings_plan_amortized_upfront_commitment_for_billing_period) 
    WHEN (line_item_line_item_type = 'RIFee') THEN (-reservation_amortized_upfront_fee_for_billing_period) 
    ELSE 0 
  END) AS sum_ri_sp_trueup,
  
  -- RI/SP upfront fees
  SUM(CASE
    WHEN (line_item_line_item_type = 'SavingsPlanUpfrontFee') THEN line_item_unblended_cost
    WHEN ((line_item_line_item_type = 'Fee') AND (reservation_reservation_a_r_n != '')) THEN line_item_unblended_cost 
    ELSE 0 
  END) AS sum_ri_sp_upfront_fees
  
FROM
  CUR
WHERE
  product_product_name IN ('Amazon Elasticsearch Service', 'Amazon OpenSearch Service')
  AND line_item_product_code = 'AmazonES'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  line_item_resource_id,
  product_product_family,
  product_instance_family,
  product_instance_type,
  pricing_term,
  product_storage_media,
  product_transfer_type
ORDER BY
  day_line_item_usage_start_date,
  product_product_family,
  sum_line_item_unblended_cost DESC 