-- View: resource_view
-- Dependencies: CUR
-- Description: Resource-level cost and usage details
-- Output: cur2_view/05_resource_view.parquet

CREATE OR REPLACE VIEW resource_view AS 
SELECT DISTINCT
  DATE_TRUNC('day', line_item_usage_start_date) AS usage_date,
  bill_payer_account_id AS payer_account_id,
  '{}' AS tags_json,
  line_item_usage_account_id AS linked_account_id,
  bill_billing_entity AS billing_entity,
  product['product_name'] AS product_name,
  line_item_resource_id AS resource_id,
  line_item_product_code AS product_code,
  line_item_operation AS operation,
  line_item_line_item_type AS charge_type,
  line_item_usage_type AS usage_type,
  pricing_unit AS pricing_unit,
  product['region'] AS region,
  line_item_line_item_description AS item_description,
  line_item_legal_entity AS legal_entity,
  pricing_term AS pricing_term,
  product['database_engine'] AS database_engine,
  product['deployment_option'] AS product_deployment_option,
  product_from_location AS product_from_location,
  product['group'] AS product_group,
  product_instance_type AS instance_type,
  product['instance_type_family'] AS instance_type_family,
  product['operating_system'] AS platform,
  product_product_family AS product_family,
  product_servicecode AS service,
  product['storage'] AS product_storage,
  product_to_location AS product_to_location,
  product['volume_api_name'] AS product_volume_api_name,
  reservation_reservation_a_r_n AS reservation_a_r_n,
  savings_plan_savings_plan_a_r_n AS savings_plan_a_r_n,
  SUM(savings_plan_savings_plan_effective_cost) AS savings_plan_effective_cost,
  SUM(reservation_effective_cost) AS reservation_effective_cost,
  SUM(line_item_usage_amount) AS usage_quantity,
  SUM(line_item_unblended_cost) AS unblended_cost
FROM
  CUR
WHERE (((CURRENT_DATE - INTERVAL 30 DAY) <= line_item_usage_start_date) 
       AND (line_item_resource_id <> '') 
       AND (NOT (COALESCE(line_item_operation, '') IN ('EKSPod-EC2', 'ECSTask-EC2'))))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
