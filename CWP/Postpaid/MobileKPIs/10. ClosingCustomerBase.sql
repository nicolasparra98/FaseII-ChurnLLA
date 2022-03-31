WITH
UsefulFields AS(
SELECT SAFE_CAST(dt AS DATE) AS DT, DATE_TRUNC(SAFE_CAST(dt AS DATE),Month) AS Month
,LEFT(CONCAT(ACCOUNTNO,'000000000000') ,12) AS ACT_ACCT_CD
,SAFE_CAST(SERVICENO AS INT64) AS SERVICENO
,MAX(SAFE_CAST(PARSE_DATETIME('%Y.%m.%d %H:%M:%S',STARTDATE_ACCOUNTNO) AS DATE)) AS MaxStart
,ACCOUNTNAME,NUMERO_IDENTIFICACION,SAFE_CAST(TOTAL_MRC_D AS FLOAT64) AS mrc_amt
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history` 
WHERE BIZ_UNIT_D="B2C" AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN')
GROUP BY DT,Month,ACT_ACCT_CD
,SERVICENO
,ACCOUNTNAME,NUMERO_IDENTIFICACION,mrc_amt
)
,NumberRGUsPerUser AS(
SELECT DISTINCT Month,dt,act_acct_cd,count(distinct SERVICENO) AS NumRGUs
FROM UsefulFields
GROUP BY Month,dt,act_acct_cd
)
,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, act_acct_cd, avg(mrc_amt) AS AvgMRC
  FROM UsefulFields 
  WHERE mrc_amt IS NOT NULL AND mrc_amt <> 0
  GROUP BY Month, act_acct_cd
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(u.DT,INTERVAL 1 MONTH),Month) AS Month, u.ACT_ACCT_CD AS accountBOM,u.dt
,mrc_amt AS B_MRC, AvgMRC AS B_Avg_MRC,NumRGUs AS B_NumRGUs,MaxStart AS B_MaxStart
FROM UsefulFields u INNER JOIN NumberRGUsPerUser n ON u.ACT_ACCT_CD=n.ACT_ACCT_CD AND u.dt=n.dt 
 LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
WHERE u.dt=LAST_DAY(u.dt,Month)
GROUP BY 1,2,3,B_MRC,B_Avg_MRC,B_NumRGUs,B_MaxStart
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(u.DT,MONTH) AS Month, u.ACT_ACCT_CD AS accountEOM,u.dt
,mrc_amt AS E_MRC, AvgMRC AS E_Avg_MRC,NumRGUs AS E_NumRGUs,MaxStart AS E_MaxStart
FROM UsefulFields u INNER JOIN NumberRGUsPerUser n ON u.ACT_ACCT_CD=n.ACT_ACCT_CD AND u.dt=n.dt 
 LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
WHERE u.dt=LAST_DAY(u.dt,Month)
GROUP BY 1,2,3,E_MRC,E_Avg_MRC,E_NumRGUs,E_MaxStart
)
,CustomerStatus AS(
  SELECT DISTINCT
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Month,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS account
  ,CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM
  ,CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM
  ,B_MRC,B_Avg_MRC,B_NumRGUs,B_MaxStart
  ,E_MRC,E_Avg_MRC,E_NumRGUs,E_MaxStart
  FROM ActiveUsersBOM b FULL OUTER JOIN ActiveUsersEOM e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH
)
,MainMovementBase AS(
SELECT DISTINCT *
,CASE WHEN (E_NumRGUs - B_NumRGUs) = 0 THEN "1.SameRGUs" 
      WHEN (E_NumRGUs - B_NumRGUs) > 0 THEN "2.Upsell"
      WHEN (E_NumRGUs - B_NumRGUs) < 0 THEN "3.Downsell"
      WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (E_MaxStart, MONTH) = '2022-02-01') THEN "4.New Customer"
      WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (E_MaxStart, MONTH) <> '2022-02-01') THEN "5.Come Back to Life"
      WHEN (B_NumRGUs > 0 AND E_NumRGUs IS NULL) THEN "6.Null last day"
      WHEN B_NumRGUs IS NULL AND E_NumRGUs IS NULL THEN "7.Always null"
 END AS MainMovement
FROM CustomerStatus
)
--SELECT DISTINCT * FROM MainMovementBase 
--ORDER BY B_NumRGUs desc, E_NumRGUs desc

--/* 10. Customer Closing Base
SELECT DISTINCT Month,COUNT(DISTINCT account) AS Records
FROM MainMovementBase
WHERE ActiveEOM=1
GROUP BY Month ORDER BY Month
--*/
