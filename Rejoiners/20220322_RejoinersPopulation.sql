WITH 

Fields AS(
 SELECT DISTINCT act_acct_cd,dt,fi_outst_age,
 FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_fixed_historic_v2` 
 WHERE org_cntry="Jamaica" AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard')
  AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
  AND fi_tot_mrc_amt IS NOT NULL AND SAFE_CAST(fi_tot_mrc_amt AS FLOAT64) <> 0
 GROUP BY act_acct_cd,dt,fi_outst_age
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(dt, INTERVAL 1 MONTH),MONTH) AS Month, act_acct_cd AS accountBOM,dt
FROM Fields
WHERE (fi_outst_age<=90 OR fi_outst_age IS NULL) AND DATE(dt) = LAST_DAY(dt, MONTH)
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, act_acct_cd AS accountEOM,dt
FROM Fields
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
,PotentialRejoiner AS(
SELECT *
,CASE WHEN ActiveBOM=1 AND ActiveEOM=0 THEN DATE_ADD(Month, INTERVAL 4 MONTH) END AS PR
FROM CustomerStatus
ORDER BY account,Month
)
,PotentialRejoinersFeb AS(
SELECT *
,CASE WHEN PR>='2022-02-01' AND PR<=DATE_ADD('2022-02-01',INTERVAL 4 MONTH) THEN 1 ELSE 0 END AS PRFeb
FROM PotentialRejoiner
)
SELECT COUNT(DISTINCT account) FROM PotentialRejoinersFeb 
WHERE PRfeb=1

