-- View: ri_sp_mapping
-- Dependencies: CUR
-- Description: Reserved Instance and Savings Plan mapping with terms and options
-- Output: cur2_view/04_ri_sp_mapping.parquet

CREATE OR REPLACE VIEW ri_sp_mapping AS 
SELECT DISTINCT
  a.billing_period_mapping,
  a.payer_account_id_mapping,
  a.ri_sp_arn_mapping,
  a.ri_sp_end_date,
  COALESCE(b.ri_sp_term, a.ri_sp_term) AS ri_sp_term,
  COALESCE(b.ri_sp_offering, a.ri_sp_offering) AS ri_sp_offering,
  COALESCE(b.ri_sp_payment, a.ri_sp_payment) AS ri_sp_payment
FROM
  ((
   SELECT DISTINCT
     bill_billing_period_start_date AS billing_period_mapping,
     bill_payer_account_id AS payer_account_id_mapping,
     (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN savings_plan_savings_plan_a_r_n 
           WHEN (reservation_reservation_a_r_n <> '') THEN reservation_reservation_a_r_n 
           ELSE '' END) AS ri_sp_arn_mapping,
     (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN CAST(CAST(STRPTIME(savings_plan_end_time, '%Y-%m-%dT%H:%M:%S.%fZ') AS DATE) AS TIMESTAMP) 
           WHEN ((reservation_reservation_a_r_n <> '') AND (reservation_end_time <> '')) THEN CAST(CAST(STRPTIME(reservation_end_time, '%Y-%m-%dT%H:%M:%S.%fZ') AS DATE) AS TIMESTAMP) 
           ELSE NULL END) AS ri_sp_end_date,
     (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN savings_plan_purchase_term 
           WHEN (reservation_reservation_a_r_n <> '') THEN pricing_lease_contract_length 
           ELSE '' END) AS ri_sp_term,
     (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN savings_plan_offering_type 
           WHEN (reservation_reservation_a_r_n <> '') THEN pricing_offering_class 
           ELSE '' END) AS ri_sp_offering,
     (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN savings_plan_payment_option 
           WHEN (reservation_reservation_a_r_n <> '') THEN pricing_purchase_option 
           ELSE '' END) AS ri_sp_payment
   FROM
     CUR
   WHERE ((line_item_line_item_type = 'RIFee') OR (line_item_line_item_type = 'SavingsPlanRecurringFee'))
)  a
LEFT JOIN (
   SELECT DISTINCT
     bill_billing_period_start_date AS billing_period_mapping,
     bill_payer_account_id AS payer_account_id_mapping,
     (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN savings_plan_savings_plan_a_r_n 
           WHEN (reservation_reservation_a_r_n <> '') THEN reservation_reservation_a_r_n 
           ELSE '' END) AS ri_sp_arn_mapping,
     (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN savings_plan_purchase_term 
           WHEN (reservation_reservation_a_r_n <> '') THEN pricing_lease_contract_length 
           ELSE '' END) AS ri_sp_term,
     (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN savings_plan_offering_type 
           WHEN (reservation_reservation_a_r_n <> '') THEN pricing_offering_class 
           ELSE '' END) AS ri_sp_offering,
     (CASE WHEN (savings_plan_savings_plan_a_r_n <> '') THEN savings_plan_payment_option 
           WHEN (reservation_reservation_a_r_n <> '') THEN pricing_purchase_option 
           ELSE '' END) AS ri_sp_payment
   FROM
     CUR
   WHERE ((line_item_line_item_type = 'DiscountedUsage') OR (line_item_line_item_type = 'SavingsPlanCoveredUsage'))
)  b ON (((a.ri_sp_arn_mapping = b.ri_sp_arn_mapping) AND (a.billing_period_mapping = b.billing_period_mapping)) AND (a.payer_account_id_mapping = b.payer_account_id_mapping)))
