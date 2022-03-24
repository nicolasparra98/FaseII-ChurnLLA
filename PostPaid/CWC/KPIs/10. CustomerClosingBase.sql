WITH 
MobileClosingBase AS(
SELECT DISTINCT account_id,DATE_TRUNC(dt,Month) AS Month
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE country_name="nan" AND dt=LAST_DAY(dt,Month)
 AND (region_name LIKE "%jama%" OR region_name LIKE "%JAMA%" OR region_name LIKE "%Jama%" OR region_name LIKE "%Jami%" 
 OR region_name LIKE "%aica%" OR region_name LIKE "%KINGSTON%"OR region_name LIKE "%Kingston%"OR region_name LIKE "%kingston%")
 AND (DATE_DIFF(safe_cast(dt as date),safe_cast(lst_pymt_dt as date),DAY)<=90 OR (dt is null OR lst_pymt_dt is null))
 AND account_status IN('Working','In Default','Promise to pay','TOSd')
 AND account_type ="Residential"
)
SELECT Month, COUNT(DISTINCT account_id) AS Records
FROM MobileClosingBase 
GROUP BY Month ORDER BY Month
