WITH 
CustomersPerMonth AS(
SELECT DISTINCT date_trunc(dt,month) AS Month, SUBSCRIPTION_CEASE_REASON, SUBSCRIPTION_CEASE_type,count(distinct account_id) AS Records
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND account_type ="Residential"
GROUP BY Month,SUBSCRIPTION_CEASE_REASON, SUBSCRIPTION_CEASE_type 
ORDER BY Month,SUBSCRIPTION_CEASE_REASON, SUBSCRIPTION_CEASE_type
)
SELECT * FROM CustomersPerMonth
WHERE SUBSCRIPTION_CEASE_REASON="Involuntary"
 OR SUBSCRIPTION_CEASE_REASON="Voluntary" 
