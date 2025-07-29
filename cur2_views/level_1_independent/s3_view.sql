-- View: s3_view
-- Dependencies: CUR
-- Description: S3 storage usage and cost analysis
-- Output: cur2_view/03_s3_view.parquet

-- CREATE OR REPLACE VIEW s3_view AS 
SELECT DISTINCT
  SPLIT_PART(billing_period, '-', 1) AS year,
  SPLIT_PART(billing_period, '-', 2) AS month,
  '{}' AS tags_json,
  bill_billing_period_start_date AS billing_period,
  DATE_TRUNC('day', line_item_usage_start_date) AS usage_date,
  bill_payer_account_id AS payer_account_id,
  line_item_usage_account_id AS linked_account_id,
  line_item_resource_id AS resource_id,
  line_item_product_code AS product_code,
  line_item_operation AS operation,
  product['region'] AS region,
  line_item_line_item_type AS charge_type,
  pricing_unit AS pricing_unit,
  SUM((CASE WHEN (line_item_line_item_type = 'Usage') THEN line_item_usage_amount ELSE 0 END)) AS usage_quantity,
  SUM(line_item_unblended_cost) AS unblended_cost,
  SUM(pricing_public_on_demand_cost) AS public_cost
FROM
  CUR
WHERE (((bill_billing_period_start_date >= (DATE_TRUNC('month', CURRENT_TIMESTAMP) - INTERVAL 3 MONTH)) 
        AND (line_item_usage_start_date < (DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL 1 DAY))) 
       AND (line_item_operation LIKE '%Storage%') 
       AND ((line_item_product_code LIKE '%AmazonGlacier%') OR (line_item_product_code LIKE '%AmazonS3%')))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
