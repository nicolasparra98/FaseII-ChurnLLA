WITH 

UsefulFields AS(
SELECT account_id,dt,total_mrc_mo
 ,DATE_DIFF(safe_cast(dt as date),safe_cast(lst_pymt_dt as date),DAY) AS fi_outst_age
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND account_type ="Residential"
AND total_mrc_mo IS NOT NULL AND NOT IS_NAN(total_mrc_mo) AND total_mrc_mo <> 0 
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(dt, INTERVAL 1 MONTH),MONTH) AS Month, account_id AS accountBOM,dt,total_mrc_mo AS mrcBOM
FROM UsefulFields
WHERE (fi_outst_age<=90 OR fi_outst_age IS NULL) AND DATE(dt) = LAST_DAY(dt, MONTH)
GROUP BY 1,2,3,4
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, account_id AS accountEOM,dt,total_mrc_mo as mrcEOM
FROM UsefulFields
WHERE fi_outst_age<=90 OR fi_outst_age IS NULL AND DATE(dt) = LAST_DAY(DATE(dt), MONTH)
GROUP BY 1,2,3,4
)
,CustomerStatus AS(
  SELECT DISTINCT mrcBOM,mrcEOM
   ,CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
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
SELECT DISTINCT Account,Month,mrcBOM,mrcEOM
,CASE WHEN ActiveBOM=1 AND ActiveEOM=1 THEN "1.Mantain"
     WHEN ActiveBOM=1 AND ActiveEOM=0 THEN "2.Loss"
     WHEN ActiveBOM=0 AND ActiveEOM=1 THEN "3.Gain"
     ELSE "4.Null" END AS GainLossFlag
FROM CustomerStatus
)
,SpinClass AS(
SELECT DISTINCT Account,Month,GainLossFlag,mrcBOM,mrcEOM,(mrcEOM-mrcBOM) AS mrcDif
,CASE WHEN GainLossFlag="1.Mantain" AND (mrcEOM-mrcBOM)=0 THEN "1.Same"
      WHEN GainLossFlag="1.Mantain" AND (mrcEOM-mrcBOM)>0 THEN "2.Upspin"
      WHEN GainLossFlag="1.Mantain" AND (mrcEOM-mrcBOM)<0 THEN "3.Downspin"
      ELSE "4.NoSpin" END AS SpinFlag
FROM CLassification
)
SELECT DISTINCT Month
--,GainLossFlag
,SpinFlag
,ROUND(SUM(mrcDif)) AS Revenue
,ROUND(SUM(mrcDif)/COUNT(DISTINCT account),2) AS ARPU
,COUNT(DISTINCT account) AS Records
FROM SpinClass
GROUP BY Month
--,GainLossFlag
,SpinFlag
ORDER BY Month
--,GainLossFlag
,SpinFlag

