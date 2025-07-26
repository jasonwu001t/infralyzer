-- EC2 Spot Instance Average Savings Analysis
-- Description: Average monthly savings for Spot Instances compared to On-Demand pricing
-- Output: Instance type, region, AZ, and average percentage savings
-- Source: AWS CUR Query Library - Cost Optimization section
-- Note: Simplified for Polars compatibility

SELECT 
  product_instance_type, 
  product_region, 
  product_availability_zone, 
  line_item_line_item_type, 
  EXTRACT(MONTH FROM line_item_usage_start_date) AS month,
  ROUND(AVG(1 - (line_item_unblended_cost / NULLIF(pricing_public_on_demand_cost, 0))) * 100, 2) AS avg_percentage_savings
FROM 
  CUR
WHERE 
  line_item_usage_type LIKE '%SpotUsage%'
  AND line_item_product_code = 'AmazonEC2'
  AND line_item_line_item_type = 'Usage'
GROUP BY 
  product_instance_type, 
  product_region, 
  product_availability_zone, 
  line_item_line_item_type, 
  EXTRACT(MONTH FROM line_item_usage_start_date)
ORDER BY 
  product_instance_type,
  product_availability_zone 