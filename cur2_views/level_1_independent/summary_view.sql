-- View: summary_view  
-- Dependencies: CUR
-- Description: Comprehensive cost summary with purchase options and aggregations
-- Output: cur2_view/02_summary_view.parquet

-- CREATE OR REPLACE VIEW summary_view AS 
SELECT
  SPLIT_PART(billing_period, '-', 1) AS year,
  SPLIT_PART(billing_period, '-', 2) AS month,
  '{}' AS tags_json,
  bill_billing_period_start_date AS billing_period,
  (CASE WHEN (DATE_TRUNC('month', line_item_usage_start_date) >= (DATE_TRUNC('month', CURRENT_TIMESTAMP) - INTERVAL 3 MONTH)) 
        THEN DATE_TRUNC('day', line_item_usage_start_date) 
        ELSE DATE_TRUNC('month', line_item_usage_start_date) END) AS usage_date,
  bill_payer_account_id AS payer_account_id,
  line_item_usage_account_id AS linked_account_id,
  bill_invoice_id AS invoice_id,
  line_item_line_item_type AS charge_type,
  (CASE WHEN (line_item_line_item_type = 'DiscountedUsage') THEN 'Running_Usage' 
        WHEN (line_item_line_item_type = 'SavingsPlanCoveredUsage') THEN 'Running_Usage' 
        WHEN (line_item_line_item_type = 'Usage') THEN 'Running_Usage' 
        ELSE 'non_usage' END) AS charge_category,
  (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN 'SavingsPlan' 
        WHEN (reservation_reservation_a_r_n <> '') THEN 'Reserved' 
        WHEN (line_item_usage_type LIKE '%Spot%') THEN 'Spot' 
        ELSE 'OnDemand' END) AS purchase_option,
  (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN savings_plan_savings_plan_a_r_n 
        WHEN (reservation_reservation_a_r_n <> '') THEN reservation_reservation_a_r_n 
        ELSE CAST('' AS VARCHAR) END) AS ri_sp_arn,
  line_item_product_code AS product_code,
  product['product_name'] AS product_name,
  (CASE WHEN ((bill_billing_entity = 'AWS Marketplace') AND (NOT (line_item_line_item_type LIKE '%Discount%'))) THEN product['product_name'] 
        WHEN (product_servicecode = '') THEN line_item_product_code 
        ELSE product_servicecode END) AS service,
  product_product_family AS product_family,
  line_item_usage_type AS usage_type,
  line_item_operation AS operation,
  line_item_line_item_description AS item_description,
  line_item_availability_zone AS availability_zone,
  product['region'] AS region,
  (CASE WHEN ((line_item_usage_type LIKE '%Spot%') AND (line_item_product_code = 'AmazonEC2') AND (line_item_line_item_type = 'Usage')) 
        THEN SPLIT_PART(line_item_line_item_description, '.', 1) 
        ELSE product['instance_type_family'] END) AS instance_type_family,
  (CASE WHEN ((line_item_usage_type LIKE '%Spot%') AND (line_item_product_code = 'AmazonEC2') AND (line_item_line_item_type = 'Usage')) 
        THEN SPLIT_PART(line_item_line_item_description, ' ', 1) 
        ELSE product_instance_type END) AS instance_type,
  (CASE WHEN ((line_item_usage_type LIKE '%Spot%') AND (line_item_product_code = 'AmazonEC2') AND (line_item_line_item_type = 'Usage')) 
        THEN SPLIT_PART(SPLIT_PART(line_item_line_item_description, ' ', 2), '/', 1) 
        ELSE product['operating_system'] END) AS platform,
  product['tenancy'] AS tenancy,
  product['physical_processor'] AS processor,
  product['processor_features'] AS processor_features,
  product['database_engine'] AS database_engine,
  product['group'] AS product_group,
  product_from_location AS product_from_location,
  product_to_location AS product_to_location,
  product['current_generation'] AS current_generation,
  line_item_legal_entity AS legal_entity,
  bill_billing_entity AS billing_entity,
  pricing_unit AS pricing_unit,
  APPROX_COUNT_DISTINCT(line_item_resource_id) AS resource_id_count,
  SUM((CASE WHEN (line_item_line_item_type = 'SavingsPlanCoveredUsage') THEN line_item_usage_amount 
            WHEN (line_item_line_item_type = 'DiscountedUsage') THEN line_item_usage_amount 
            WHEN (line_item_line_item_type = 'Usage') THEN line_item_usage_amount 
            ELSE 0 END)) AS usage_quantity,
  SUM(line_item_unblended_cost) AS unblended_cost,
  SUM((CASE WHEN (line_item_line_item_type = 'SavingsPlanCoveredUsage') THEN savings_plan_savings_plan_effective_cost 
            WHEN (line_item_line_item_type = 'SavingsPlanRecurringFee') THEN (savings_plan_total_commitment_to_date - savings_plan_used_commitment) 
            WHEN (line_item_line_item_type = 'SavingsPlanNegation') THEN 0 
            WHEN (line_item_line_item_type = 'SavingsPlanUpfrontFee') THEN 0 
            WHEN (line_item_line_item_type = 'DiscountedUsage') THEN reservation_effective_cost 
            WHEN (line_item_line_item_type = 'RIFee') THEN (reservation_unused_amortized_upfront_fee_for_billing_period + reservation_unused_recurring_fee) 
            WHEN ((line_item_line_item_type = 'Fee') AND (reservation_reservation_a_r_n <> '')) THEN 0 
            WHEN ((line_item_line_item_type = 'Refund') AND (line_item_product_code = 'ComputeSavingsPlans')) THEN 0 
            ELSE line_item_unblended_cost END)) AS amortized_cost,
  SUM((CASE WHEN (line_item_line_item_type = 'SavingsPlanRecurringFee') THEN -(savings_plan_amortized_upfront_commitment_for_billing_period) 
            WHEN (line_item_line_item_type = 'RIFee') THEN -(reservation_amortized_upfront_fee_for_billing_period) 
            ELSE 0 END)) AS ri_sp_trueup,
  SUM((CASE WHEN (line_item_line_item_type = 'SavingsPlanUpfrontFee') THEN line_item_unblended_cost 
            WHEN ((line_item_line_item_type = 'Fee') AND (reservation_reservation_a_r_n <> '')) THEN line_item_unblended_cost 
            ELSE 0 END)) AS ri_sp_upfront_fees,
  SUM((CASE WHEN (line_item_line_item_type <> 'SavingsPlanNegation') THEN pricing_public_on_demand_cost 
            ELSE 0 END)) AS public_cost
FROM
  CUR
WHERE ((bill_billing_period_start_date >= (DATE_TRUNC('month', CURRENT_TIMESTAMP) - INTERVAL 7 MONTH)) 
       AND (CAST(CONCAT(billing_period, '-01') AS DATE) >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 7 MONTH)) 
       AND (NOT (COALESCE(line_item_operation, '') IN ('EKSPod-EC2', 'ECSTask-EC2'))))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35
