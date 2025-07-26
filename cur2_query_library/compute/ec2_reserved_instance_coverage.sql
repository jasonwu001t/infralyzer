-- EC2 Reserved Instance Coverage Analysis
-- Description: Reserved Instance coverage for EC2 with utilization and On-Demand usage details
-- Output: Purchase options, instance types, usage amounts, and cost details
-- Source: AWS CUR Query Library - Compute section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  CASE 
    WHEN line_item_line_item_type IN ('Usage') THEN 'OnDemand'
    WHEN line_item_line_item_type IN ('Fee','RIFee','DiscountedUsage') THEN 'ReservedInstance' 
  END AS case_purchase_option,
  SPLIT_PART(SPLIT_PART(reservation_reservation_a_r_n, ':', 6), '/', 2) AS split_reservation_reservation_a_r_n,
  SPLIT_PART(line_item_usage_type, ':', 2) AS split_line_item_usage_type_instance_type,
  SPLIT_PART(SPLIT_PART(line_item_usage_type, ':', 2), '.', 1) AS split_line_item_usage_type_instance_family,
  CASE 
    WHEN product_region IS NULL OR product_region = '' THEN 'Global'
    ELSE product_region
  END AS case_product_region,
  line_item_line_item_type,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(reservation_unused_quantity) AS sum_reservation_unused_quantity,
  SUM(line_item_normalized_usage_amount) AS sum_line_item_normalized_usage_amount,
  SUM(reservation_unused_normalized_unit_quantity) AS sum_reservation_unused_normalized_unit_quantity,
  SUM(reservation_effective_cost) AS sum_reservation_effective_cost,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM 
  CUR 
WHERE 
  product_product_name = 'Amazon Elastic Compute Cloud'
  AND line_item_operation LIKE '%RunInstance%'
  AND line_item_line_item_type IN ('Usage','Fee','RIFee','DiscountedUsage')
  AND product_product_family NOT IN ('Data Transfer')
GROUP BY 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date,
  CASE 
    WHEN line_item_line_item_type IN ('Usage') THEN 'OnDemand'
    WHEN line_item_line_item_type IN ('Fee','RIFee','DiscountedUsage') THEN 'ReservedInstance' 
  END,
  SPLIT_PART(SPLIT_PART(reservation_reservation_a_r_n, ':', 6), '/', 2),
  SPLIT_PART(line_item_usage_type, ':', 2),
  SPLIT_PART(SPLIT_PART(line_item_usage_type, ':', 2), '.', 1),
  CASE 
    WHEN product_region IS NULL OR product_region = '' THEN 'Global'
    ELSE product_region
  END,
  line_item_line_item_type
ORDER BY 
  day_line_item_usage_start_date,
  split_line_item_usage_type_instance_type,
  sum_line_item_unblended_cost DESC 