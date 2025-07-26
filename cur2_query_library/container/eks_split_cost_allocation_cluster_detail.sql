-- Amazon EKS Split Cost Allocation Data Cluster Detail Analysis
-- Description: Daily CPU, RAM, and total split cost per pod on all clusters with cluster-level metadata
-- Output: Pod-level cost breakdown with cluster, node, namespace, and deployment details
-- Source: AWS CUR Query Library - Container section
-- Note: Simplified for Polars compatibility

SELECT 
  line_item_usage_start_date::date AS day,
  line_item_resource_id AS resource_id,
  split_line_item_parent_resource_id AS node_instance_id,
  SPLIT_PART(line_item_resource_id, '/', 2) AS cluster_name,
  SPLIT_PART(line_item_resource_id, '/', 3) AS namespace,
  SPLIT_PART(line_item_resource_id, '/', 4) AS pod_name,
  SUM(CASE WHEN line_item_usage_type LIKE '%EKS-EC2-vCPU-Hours' THEN split_line_item_split_cost + split_line_item_unused_cost ELSE 0.0 END) AS cpu_cost,
  SUM(CASE WHEN line_item_usage_type LIKE '%EKS-EC2-GB-Hours' THEN split_line_item_split_cost + split_line_item_unused_cost ELSE 0.0 END) AS ram_cost,
  SUM(split_line_item_split_cost + split_line_item_unused_cost) AS total_cost
FROM 
  CUR
WHERE 
  line_item_operation = 'EKSPod-EC2'
  AND line_item_usage_start_date::date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 
  line_item_usage_start_date::date,
  line_item_resource_id,
  split_line_item_parent_resource_id,
  SPLIT_PART(line_item_resource_id, '/', 2),
  SPLIT_PART(line_item_resource_id, '/', 3),
  SPLIT_PART(line_item_resource_id, '/', 4)
ORDER BY 
  cluster_name,
  day DESC 