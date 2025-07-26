-- Amazon RDS Daily Cost and Usage Analysis
-- Description: Daily sum per resource for all RDS purchase options and usage types
-- Output: Daily cost, usage, and amortized cost by resource and type
-- Source: AWS CUR Query Library - Database section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  product_instance_type, 
  line_item_operation, 
  line_item_usage_type, 
  line_item_line_item_type,
  pricing_term, 
  product_product_family, 
  SPLIT_PART(line_item_resource_id, ':', 7) AS split_line_item_resource_id,
  product_database_engine,
  SUM(CASE WHEN line_item_line_item_type = 'DiscountedUsage' THEN line_item_usage_amount
    WHEN line_item_line_item_type = 'Usage' THEN line_item_usage_amount
    ELSE 0 
  END) AS sum_line_item_usage_amount, 
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost, 
  SUM(CASE WHEN line_item_line_item_type = 'DiscountedUsage' THEN reservation_effective_cost
    WHEN line_item_line_item_type = 'RIFee' THEN reservation_unused_amortized_upfront_fee_for_billing_period + reservation_unused_recurring_fee
    WHEN line_item_line_item_type = 'Fee' AND reservation_reservation_a_r_n <> '' THEN 0
    ELSE line_item_unblended_cost 
  END) AS sum_amortized_cost, 
  SUM(CASE WHEN line_item_line_item_type = 'RIFee' THEN -reservation_amortized_upfront_fee_for_billing_period
    ELSE 0 
  END) AS sum_ri_trueup, 
  SUM(CASE WHEN line_item_line_item_type = 'Fee' AND reservation_reservation_a_r_n <> '' THEN line_item_unblended_cost 
    ELSE 0 
  END) AS sum_ri_upfront_fees
FROM 
  CUR
WHERE 
  product_product_name = 'Amazon Relational Database Service'
  AND line_item_line_item_type  IN ('DiscountedUsage', 'Usage', 'Fee', 'RIFee')
GROUP BY 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date, 
  product_instance_type, 
  line_item_operation, 
  line_item_usage_type, 
  line_item_line_item_type,
  pricing_term, 
  product_product_family, 
  SPLIT_PART(line_item_resource_id, ':', 7),
  product_database_engine
ORDER BY 
  day_line_item_usage_start_date, 
  sum_line_item_usage_amount, 
  sum_amortized_cost 