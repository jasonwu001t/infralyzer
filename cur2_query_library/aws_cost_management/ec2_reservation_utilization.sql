-- EC2 Reservation Utilization Analysis
-- Description: Active EC2 Reserved Instance ARNs and their utilization for analysis
-- Output: Detailed reservation metrics including utilization percentages and expiration dates
-- Source: AWS CUR Query Library - AWS Cost Management section
-- Note: Simplified for Polars compatibility - some date filtering may need adjustment

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') AS month_line_item_usage_start_date,
  bill_bill_type,
  line_item_product_code,
  line_item_usage_type,
  product_region,
  reservation_subscription_id,
  reservation_reservation_a_r_n,
  pricing_purchase_option,
  pricing_offering_class,
  pricing_lease_contract_length,
  reservation_number_of_reservations,
  reservation_start_time,
  reservation_end_time,
  reservation_modification_status,
  reservation_total_reserved_units,
  reservation_unused_quantity,
  CASE 
    WHEN reservation_total_reserved_units > 0 THEN 
      1 - (reservation_unused_quantity / reservation_total_reserved_units)
    ELSE 0
  END AS calc_percentage_utilized
FROM
  CUR
WHERE 
  pricing_term = 'Reserved'
  AND line_item_line_item_type IN ('Fee','RIFee')
  AND line_item_product_code = 'AmazonEC2'
  AND bill_bill_type = 'Anniversary'
  AND reservation_total_reserved_units > 0
GROUP BY 
  bill_bill_type,
  bill_payer_account_id,
  line_item_usage_account_id,
  reservation_reservation_a_r_n,
  reservation_subscription_id,
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0'),
  line_item_product_code,
  line_item_usage_type,
  product_region,
  pricing_purchase_option,
  pricing_offering_class,
  pricing_lease_contract_length,
  reservation_number_of_reservations,
  reservation_start_time,
  reservation_end_time,
  reservation_modification_status,
  reservation_total_reserved_units,
  reservation_unused_quantity
ORDER BY 
  reservation_unused_quantity DESC,
  reservation_end_time ASC,
  calc_percentage_utilized ASC 