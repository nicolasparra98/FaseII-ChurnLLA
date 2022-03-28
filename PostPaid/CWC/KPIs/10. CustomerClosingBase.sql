WITH 
MobileClosingBase AS(
SELECT DISTINCT account_id,DATE_TRUNC(dt,Month) AS Month
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND dt=LAST_DAY(dt,Month) AND account_type ="Residential"
 AND account_status NOT IN('Ceased','Closed','Recommended for cease')
 AND total_mrc_mo IS NOT NULL AND NOT IS_NAN(total_mrc_mo) AND total_mrc_mo <> 0 
)
SELECT Month, COUNT(DISTINCT account_id) AS Records
FROM MobileClosingBase 
GROUP BY Month ORDER BY Month
