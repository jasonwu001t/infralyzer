-- Amazon EKS Split Cost Allocation Data Monthly Cost per Namespace per Cluster Analysis
-- Description: Monthly CPU, RAM, and total split cost per namespace per cluster
-- Output: Namespace-level cost breakdown by cluster and month
-- Source: AWS CUR Query Library - Container section
-- Note: Simplified for Polars compatibility

SELECT 
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') AS month,
  SPLIT_PART(line_item_resource_id, '/', 2) AS cluster_name,
  SPLIT_PART(line_item_resource_id, '/', 3) AS namespace,
  SUM(CASE WHEN line_item_usage_type LIKE '%EKS-EC2-vCPU-Hours' THEN split_line_item_split_cost + split_line_item_unused_cost ELSE 0.0 END) AS cpu_cost,
  SUM(CASE WHEN line_item_usage_type LIKE '%EKS-EC2-GB-Hours' THEN split_line_item_split_cost + split_line_item_unused_cost ELSE 0.0 END) AS ram_cost,
  SUM(split_line_item_split_cost + split_line_item_unused_cost) AS total_cost
FROM 
  CUR
WHERE 
  line_item_operation = 'EKSPod-EC2'
GROUP BY 
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0'),
  SPLIT_PART(line_item_resource_id, '/', 2),
  SPLIT_PART(line_item_resource_id, '/', 3)
ORDER BY 
  month DESC,
  cluster_name,
  namespace,
  total_cost DESC 