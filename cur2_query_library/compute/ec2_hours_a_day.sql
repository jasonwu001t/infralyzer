-- EC2 Hours a Day Analysis
-- Description: EC2 usage quantity in hours for each purchase option and instance type
-- Output: Instance types, purchase options, amortized costs, and usage quantities
-- Source: AWS CUR Query Library - Compute section

SELECT 
  bill_billing_period_start_date,
  line_item_usage_start_date, 
  bill_payer_account_id, 
  line_item_usage_account_id,
  CASE 
    WHEN line_item_usage_type LIKE '%SpotUsage%' THEN SPLIT_PART(line_item_usage_type, ':', 2)
    ELSE product_instance_type
  END AS case_product_instance_type,
  CASE
    WHEN (savings_plan_savings_plan_a_r_n <> '' AND savings_plan_savings_plan_a_r_n IS NOT NULL) THEN 'SavingsPlan'
    WHEN (reservation_reservation_a_r_n <> '' AND reservation_reservation_a_r_n IS NOT NULL) THEN 'Reserved'
    WHEN line_item_usage_type LIKE '%Spot%' THEN 'Spot'
    ELSE 'OnDemand' 
  END AS case_purchase_option, 
  SUM(CASE
    WHEN line_item_line_item_type = 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost
    WHEN line_item_line_item_type = 'DiscountedUsage' THEN reservation_effective_cost
    WHEN line_item_line_item_type = 'Usage' THEN line_item_unblended_cost
    ELSE 0 
  END) AS sum_amortized_cost, 
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount
FROM 
  CUR  
WHERE 
  line_item_product_code = 'AmazonEC2'
  AND product_servicecode <> 'AWSDataTransfer'
  AND line_item_operation LIKE '%RunInstances%'
  AND line_item_usage_type NOT LIKE '%DataXfer%'
  AND line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage')
  AND (product_capacitystatus != 'AllocatedCapacityReservation' OR product_capacitystatus IS NULL)
GROUP BY 
  bill_billing_period_start_date,
  line_item_usage_start_date, 
  bill_payer_account_id, 
  line_item_usage_account_id,
  CASE 
    WHEN line_item_usage_type LIKE '%SpotUsage%' THEN SPLIT_PART(line_item_usage_type, ':', 2)
    ELSE product_instance_type
  END,
  CASE
    WHEN (savings_plan_savings_plan_a_r_n <> '' AND savings_plan_savings_plan_a_r_n IS NOT NULL) THEN 'SavingsPlan'
    WHEN (reservation_reservation_a_r_n <> '' AND reservation_reservation_a_r_n IS NOT NULL) THEN 'Reserved'
    WHEN line_item_usage_type LIKE '%Spot%' THEN 'Spot'
    ELSE 'OnDemand' 
  END
ORDER BY 
  sum_line_item_usage_amount DESC 