-- Amazon Aurora Global Database Cost Breakdown
-- Description: Daily cost breakdown for Aurora Global Database deployment, grouped by resource and usage type
-- Output: Day, account, charge type, usage type, operation, description, resource ID, cost, and usage
-- Source: AWS CUR Query Library - Database section
-- Note: Replace the resource_id placeholders with your actual Aurora cluster/instance IDs

SELECT 
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  line_item_usage_account_id,
  line_item_line_item_type,
  line_item_usage_type,
  line_item_operation,
  line_item_line_item_description,
  line_item_resource_id,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount
FROM 
  CUR
WHERE 
  (
    line_item_resource_id LIKE '%{primary_cluster_id}%'
    OR line_item_resource_id LIKE '%{secondary_cluster_id_1}%'
    OR line_item_resource_id LIKE '%{secondary_cluster_id_n}%'
    OR line_item_resource_id LIKE '%{primary_cluster_db_instance_name_1}%'
    OR line_item_resource_id LIKE '%{primary_cluster_db_instance_name_n}%'
    OR line_item_resource_id LIKE '%{secondary_cluster_db_instance_name_1}%'
    OR line_item_resource_id LIKE '%{secondary_cluster_db_instance_name_n}%'
  )
  AND line_item_usage_type NOT LIKE '%BackupUsage%'
GROUP BY 
  line_item_usage_start_date::date, 
  line_item_usage_account_id,
  line_item_line_item_type,
  line_item_usage_type,
  line_item_operation,
  line_item_line_item_description,
  line_item_resource_id
ORDER BY
  day_line_item_usage_start_date, 
  sum_line_item_unblended_cost DESC 