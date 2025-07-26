-- EC2 Total Spend Analysis
-- Description: Top costs for all spend with product code AmazonEC2 including all pricing categories
-- Output: Product code, line item description, and total unblended costs ordered by spend
-- Source: AWS CUR Query Library - Compute section

SELECT 
  line_item_product_code, 
  line_item_line_item_description, 
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost 
FROM 
  CUR 
WHERE 
  line_item_product_code = 'AmazonEC2'
  AND line_item_line_item_type NOT IN ('Tax','Refund','Credit')
GROUP BY 
  line_item_product_code, 
  line_item_line_item_description
ORDER BY 
  sum_line_item_unblended_cost DESC 