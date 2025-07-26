-- Amazon EKS Split Cost Allocation Data Comparison Analysis
-- Description: Compare aggregated split amortized cost by EC2 instance with EC2 instance running hours amortized cost
-- Output: Daily total split + unused amortized cost by instance and equivalent EC2 instance running hours amortized cost
-- Source: AWS CUR Query Library - Container section
-- Note: This is a simplified version for Polars compatibility

-- This query requires joining CUR with itself, which is not natively supported in Polars SQL. You may need to run two queries and join in Python.
-- Here is a single-table version that sums split costs by instance and day.

SELECT 
  line_item_usage_start_date::date AS day,
  split_line_item_parent_resource_id AS instance_id,
  SUM(split_line_item_split_cost + split_line_item_unused_cost) AS total_split_amortized_cost
FROM 
  CUR
WHERE 
  line_item_operation = 'EKSPod-EC2'
  AND split_line_item_split_cost > 0.0
GROUP BY 
  line_item_usage_start_date::date,
  split_line_item_parent_resource_id
ORDER BY 
  day DESC,
  total_split_amortized_cost DESC 