-- Amazon SES Daily Cost and Usage Analysis
-- Description: Daily unblended cost and usage information per linked account for Amazon SES with categorized usage types
-- Output: Account details, product family, categorized usage types, and costs in descending order
-- Source: AWS CUR Query Library - Application Integration section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_usage_start_date::date AS day_line_item_usage_start_date, 
  product_product_family,
  CASE
    WHEN line_item_usage_type LIKE '%DataTransfer-In-Bytes%' THEN 'Data Transfer GB (IN) '
    WHEN line_item_usage_type LIKE '%DataTransfer-Out-Bytes%' THEN 'Data Transfer GB (Out)'
    WHEN line_item_usage_type LIKE '%AttachmentsSize-Bytes%' THEN 'Attachments GB'
    WHEN line_item_usage_type LIKE '%Recipients' THEN 'Recipients'
    WHEN line_item_usage_type LIKE '%Recipients-EC2' THEN 'Recipients'
    WHEN line_item_usage_type LIKE '%Recipients-MailboxSim' THEN 'Recipients (MailboxSimulator)'
    WHEN line_item_usage_type LIKE '%Message%' THEN 'Messages'
    WHEN line_item_usage_type LIKE '%ReceivedChunk%' THEN 'Received Chunk'
    ELSE 'Others'
  END AS case_line_item_usage_type,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM 
  CUR 
WHERE 
  product_product_name = 'Amazon Simple Email Service'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage')
GROUP BY 
  bill_payer_account_id, 
  line_item_usage_account_id,
  line_item_usage_start_date::date, 
  product_product_family,
  CASE
    WHEN line_item_usage_type LIKE '%DataTransfer-In-Bytes%' THEN 'Data Transfer GB (IN) '
    WHEN line_item_usage_type LIKE '%DataTransfer-Out-Bytes%' THEN 'Data Transfer GB (Out)'
    WHEN line_item_usage_type LIKE '%AttachmentsSize-Bytes%' THEN 'Attachments GB'
    WHEN line_item_usage_type LIKE '%Recipients' THEN 'Recipients'
    WHEN line_item_usage_type LIKE '%Recipients-EC2' THEN 'Recipients'
    WHEN line_item_usage_type LIKE '%Recipients-MailboxSim' THEN 'Recipients (MailboxSimulator)'
    WHEN line_item_usage_type LIKE '%Message%' THEN 'Messages'
    WHEN line_item_usage_type LIKE '%ReceivedChunk%' THEN 'Received Chunk'
    ELSE 'Others'
  END
ORDER BY 
  day_line_item_usage_start_date,
  sum_line_item_usage_amount,
  sum_line_item_unblended_cost DESC 