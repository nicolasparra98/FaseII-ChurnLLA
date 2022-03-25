SELECT DISTINCT DATE_TRUNC(dt,Month) AS Month
,ROUND(SUM(total_mrc_mo)) AS Revenue
,ROUND(SUM(total_mrc_mo)/COUNT(DISTINCT account_id),2) AS ARPU
,count(account_id) AS Records
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND account_type ="Residential" 
 AND account_status NOT IN('Ceased','Closed','Recommended for cease')
 AND dt=LAST_DAY(dt,Month) 
 AND total_mrc_mo IS NOT NULL AND NOT IS_NAN(total_mrc_mo) AND total_mrc_mo<>0 
GROUP BY Month
ORDER BY Month
