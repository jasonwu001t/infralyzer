-- View: ec2_running_cost
-- Dependencies: CUR
-- Description: EC2 running instance costs by purchase option (SP, RI, Spot, OD)
-- Output: cur2_view/07_ec2_running_cost.parquet

-- CREATE OR REPLACE VIEW ec2_running_cost AS 
SELECT DISTINCT
  SPLIT_PART(billing_period, '-', 1) AS year,
  SPLIT_PART(billing_period, '-', 2) AS month,
  '{}' AS tags_json,
  bill_billing_period_start_date AS billing_period,
  DATE_TRUNC('hour', line_item_usage_start_date) AS usage_date,
  bill_payer_account_id AS payer_account_id,
  line_item_usage_account_id AS linked_account_id,
  (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN 'SavingsPlan' 
        WHEN (reservation_reservation_a_r_n <> '') THEN 'Reserved' 
        WHEN (line_item_usage_type LIKE '%Spot%') THEN 'Spot' 
        ELSE 'OnDemand' END) AS purchase_option,
  SUM((CASE WHEN (line_item_line_item_type = 'SavingsPlanCoveredUsage') THEN savings_plan_savings_plan_effective_cost 
            WHEN (line_item_line_item_type = 'DiscountedUsage') THEN reservation_effective_cost 
            WHEN (line_item_line_item_type = 'Usage') THEN line_item_unblended_cost 
            ELSE 0 END)) AS amortized_cost,
  ROUND(SUM(line_item_usage_amount), 2) AS usage_quantity
FROM
  CUR
WHERE (
        (
          (
            ((bill_billing_period_start_date >= (DATE_TRUNC('month', CURRENT_TIMESTAMP) - INTERVAL 1 MONTH)) 
            AND (line_item_product_code = 'AmazonEC2')
            ) 
            AND (product_servicecode <> 'AWSDataTransfer')
          ) 
            AND (line_item_operation LIKE '%RunInstances%')
        ) 
       AND (NOT (line_item_usage_type LIKE '%DataXfer%')) 
       AND ((line_item_line_item_type = 'Usage') 
            OR (line_item_line_item_type = 'SavingsPlanCoveredUsage')
            OR (line_item_line_item_type = 'DiscountedUsage')
            )
      )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
