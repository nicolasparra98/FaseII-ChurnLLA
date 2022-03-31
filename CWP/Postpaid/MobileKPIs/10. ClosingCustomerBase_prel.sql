WITH
UsefulFields AS(
SELECT SAFE_CAST(dt AS DATE) AS DT
,LEFT(CONCAT(ACCOUNTNO,'000000000000') ,12) AS ACT_ACCT_CD,SAFE_CAST(SERVICENO AS INT64) AS SERVICENO
,ACCOUNTNAME,NUMERO_IDENTIFICACION
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history` 
WHERE BIZ_UNIT_D="B2C"
GROUP BY DT,ACT_ACCT_CD,SERVICENO,ACCOUNTNAME,NUMERO_IDENTIFICACION
)
/*,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, act_acct_cd, avg(mrc_amt) AS AvgMRC
  FROM UsefulFields 
  WHERE mrc_amt IS NOT NULL AND mrc_amt <> 0
  GROUP BY Month, act_acct_cd
)*/
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(DT,INTERVAL 1 MONTH),Month) AS Month, u.ACT_ACCT_CD AS accountBOM,dt
FROM UsefulFields u --LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
WHERE dt=LAST_DAY(dt,Month)
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DT,MONTH) AS Month, u.ACT_ACCT_CD AS accountEOM,dt
FROM UsefulFields u --LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
WHERE dt=LAST_DAY(dt,Month)
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

SELECT DISTINCT Month,COUNT(DISTINCT Account) AS Records
FROM customerstatus
GROUP BY Month
ORDER BY Month
