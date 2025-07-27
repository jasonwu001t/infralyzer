-- View: compute_savings_plan_eligible_spend
-- Dependencies: CUR
-- Description: Compute services eligible for Savings Plan discounts
-- Output: cur2_view/08_compute_savings_plan_eligible_spend.parquet

CREATE OR REPLACE VIEW compute_savings_plan_eligible_spend AS 
SELECT DISTINCT
  SPLIT_PART(billing_period, '-', 1) AS year,
  SPLIT_PART(billing_period, '-', 2) AS month,
  '{}' AS tags_json,
  bill_payer_account_id AS payer_account_id,
  line_item_usage_account_id AS linked_account_id,
  bill_billing_period_start_date AS billing_period,
  DATE_TRUNC('hour', line_item_usage_start_date) AS usage_date,
  SUM(line_item_unblended_cost) AS unblended_cost
FROM
  CUR
WHERE ((bill_billing_period_start_date >= (DATE_TRUNC('month', CURRENT_TIMESTAMP) - INTERVAL 1 MONTH)) 
       AND (line_item_usage_start_date < (DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL 1 DAY)) 
       AND (line_item_line_item_type = 'Usage') 
       AND (product_servicecode <> 'AWSDataTransfer') 
       AND (NOT (line_item_usage_type LIKE '%Spot%')) 
       AND (NOT (line_item_usage_type LIKE '%DataXfer%')) 
       AND (((line_item_product_code = 'AmazonEC2') AND (line_item_operation LIKE '%RunInstances%')) 
            OR ((line_item_product_code = 'AWSLambda') AND ((line_item_usage_type LIKE '%Lambda-GB-Second%') OR (line_item_usage_type LIKE '%Lambda-Provisioned-GB-Second%') OR (line_item_usage_type LIKE '%Lambda-Provisioned-Concurrency%'))) 
            OR (line_item_usage_type LIKE '%Fargate%')))
GROUP BY 1, 2, 3, 4, 5, 6, 7
