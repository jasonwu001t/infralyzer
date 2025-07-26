-- Amazon EKS Split Cost Allocation Data Analysis
-- Description: EKS split cost allocation for pod-level cost and resource consumption
-- Output: Split usage, split cost, and unused cost by pod, cluster, and namespace
-- Source: AWS CUR Query Library - Container section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_type,
  SPLIT_PART(line_item_resource_id, ':', 6) AS split_line_item_resource_id,
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') AS month_line_item_usage_start_date, 
  line_item_operation,
  line_item_product_code,
  split_line_item_parent_resource_id,
  SUM(split_line_item_split_usage) AS sum_split_line_item_split_usage,
  SUM(split_line_item_split_cost) AS sum_split_line_item_split_cost,
  SUM(split_line_item_unused_cost) AS sum_split_line_item_unused_cost
FROM 
  CUR
WHERE 
  line_item_product_code = 'AmazonEKS' 
  AND line_item_operation LIKE 'EKSPod%'
  AND split_line_item_split_usage IS NOT NULL
  AND split_line_item_split_cost IS NOT NULL
  AND split_line_item_unused_cost IS NOT NULL
GROUP BY 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_type,
  SPLIT_PART(line_item_resource_id, ':', 6),
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0'),
  line_item_operation,
  line_item_product_code,
  split_line_item_parent_resource_id
ORDER BY 
  month_line_item_usage_start_date ASC,
  sum_split_line_item_unused_cost DESC,
  line_item_operation 