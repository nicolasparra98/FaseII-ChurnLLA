WITH 

UsefulFields AS(
SELECT account_id,dt, total_mrc_mo as mrc_amt,max(dt) AS MaxDateMonth,MAX(account_creation_date) AS MaxStart
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND account_type ="Residential"
 AND account_status NOT IN('Ceased','Closed','Recommended for cease')
GROUP BY 1,2,3
)
,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, account_id, avg(mrc_amt) AS AvgMRC
  FROM UsefulFields
  WHERE mrc_amt IS NOT NULL AND mrc_amt <> 0
  GROUP BY Month,account_id
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(dt, INTERVAL 1 MONTH),MONTH) AS Month, account_id AS accountBOM,dt
FROM UsefulFields
WHERE dt=LAST_DAY(dt,Month)
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, u.account_id AS accountEOM,dt,avgmrc
FROM UsefulFields u LEFT JOIN AverageMRC_User a ON u.account_id = a.account_id AND DATE_TRUNC(u.dt,month) = a.Month
WHERE dt=LAST_DAY(dt,Month)
 AND ((AvgMRC IS NOT NULL AND AvgMRC <> 0 AND DATE_DIFF(MaxDateMonth, MaxStart, DAY)>60) OR  (DATE_DIFF(MaxDateMonth, MaxStart, DAY)<=60))
GROUP BY 1,2,3,4
)
,CustomerStatus AS(
  SELECT DISTINCT avgmrc,
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
SELECT DISTINCT Account,Month,avgmrc
,CASE WHEN ActiveBOM=1 AND ActiveEOM=1 THEN "1.Mantain"
     WHEN ActiveBOM=1 AND ActiveEOM=0 THEN "2.Loss"
     WHEN ActiveBOM=0 AND ActiveEOM=1 THEN "3.Gain"
     ELSE "4.Null" END AS GainLossFlag
FROM CustomerStatus
)

SELECT DISTINCT  Month,GainLossFlag,count(distinct account) AS Records
FROM Classification
GROUP BY Month,GainLossFlag
ORDER BY Month,GainLossFlag
