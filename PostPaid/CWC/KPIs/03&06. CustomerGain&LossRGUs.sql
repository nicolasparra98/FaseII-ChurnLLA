WITH 

UsefulFields AS(
SELECT account_id,dt
 ,DATE_DIFF(safe_cast(dt as date),safe_cast(lst_pymt_dt as date),DAY) AS fi_outst_age
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE country_name="nan" 
 AND (region_name LIKE "%jama%" OR region_name LIKE "%JAMA%" OR region_name LIKE "%Jama%" OR region_name LIKE "%Jami%" 
 OR region_name LIKE "%aica%" OR region_name LIKE "%KINGSTON%"OR region_name LIKE "%Kingston%"OR region_name LIKE "%kingston%")
 AND (DATE_DIFF(safe_cast(dt as date),safe_cast(lst_pymt_dt as date),DAY)<=90 OR (dt is null OR lst_pymt_dt is null))
 AND account_status IN('Working','In Default','Promise to pay','TOSd')
 AND account_type ="Residential"
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(dt, INTERVAL 1 MONTH),MONTH) AS Month, account_id AS accountBOM,dt
FROM UsefulFields
WHERE (fi_outst_age<=90 OR fi_outst_age IS NULL) AND DATE(dt) = LAST_DAY(dt, MONTH)
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, account_id AS accountEOM,dt
FROM UsefulFields
WHERE fi_outst_age<=90 OR fi_outst_age IS NULL AND DATE(dt) = LAST_DAY(DATE(dt), MONTH)
GROUP BY 1,2,3
)
,CustomerStatus AS(
  SELECT DISTINCT
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Month,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS account,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  FROM ActiveUsersBOM b FULL OUTER JOIN ActiveUsersEOM e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH
)
,Classification AS(
SELECT DISTINCT Account,Month
,CASE WHEN ActiveBOM=1 AND ActiveEOM=1 THEN "1.Mantain"
     WHEN ActiveBOM=1 AND ActiveEOM=0 THEN "2.Loss"
     WHEN ActiveBOM=0 AND ActiveEOM=1 THEN "3.Gain"
     ELSE "4.Null" END AS GainLossFlag
FROM CustomerStatus
)
SELECT DISTINCT Month,GainLossFlag,count(distinct account) AS Records
FROM Classification
GROUP BY Month,GainLossFlag
ORDER BY Month,GainLossFlag

