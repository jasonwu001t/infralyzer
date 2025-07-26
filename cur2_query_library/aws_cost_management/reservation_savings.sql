-- Reservation Savings Analysis
-- Description: Aggregated savings from reservations across EC2, Elasticache, OpenSearch, RDS, and Redshift
-- Output: Savings per reservation ARN, service, account, region, and instance type
-- Source: AWS CUR Query Library - AWS Cost Management section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') AS month_line_item_usage_start_date,
  line_item_product_code,
  reservation_reservation_a_r_n,
  SPLIT_PART(line_item_usage_type, ':', 2) AS split_line_item_usage_type,
  SPLIT_PART(reservation_reservation_a_r_n, ':', 4) AS split_product_region,
  SUM(pricing_public_on_demand_cost) AS sum_pricing_public_on_demand_cost,
  SUM(CASE
    WHEN line_item_line_item_type = 'DiscountedUsage' THEN reservation_effective_cost
    WHEN line_item_line_item_type = 'RIFee' THEN reservation_unused_amortized_upfront_fee_for_billing_period + reservation_unused_recurring_fee
    WHEN line_item_line_item_type = 'Fee' AND reservation_reservation_a_r_n <> '' THEN 0
    ELSE 0
  END) AS sum_case_reservation_effective_cost,
  SUM(pricing_public_on_demand_cost) - SUM(CASE
    WHEN line_item_line_item_type = 'DiscountedUsage' THEN reservation_effective_cost
    WHEN line_item_line_item_type = 'RIFee' THEN reservation_unused_amortized_upfront_fee_for_billing_period + reservation_unused_recurring_fee
    WHEN line_item_line_item_type = 'Fee' AND reservation_reservation_a_r_n <> '' THEN 0
    ELSE 0
  END) AS sum_case_reservation_net_savings
FROM
  CUR
WHERE
  line_item_product_code IN ('AmazonEC2','AmazonRedshift','AmazonRDS','AmazonES','AmazonElastiCache')  
  AND line_item_line_item_type IN ('Fee','RIFee','DiscountedUsage')
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0'),
  line_item_product_code,
  reservation_reservation_a_r_n,
  SPLIT_PART(line_item_usage_type, ':', 2),
  SPLIT_PART(reservation_reservation_a_r_n, ':', 4)
ORDER BY
  month_line_item_usage_start_date,
  line_item_product_code,
  split_line_item_usage_type 