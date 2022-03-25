WITH 

UsefulFields AS(
SELECT account_id,dt
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND account_type ="Residential"
 AND account_status NOT IN('Ceased','Closed','Recommended for cease')
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(dt, INTERVAL 1 MONTH),MONTH) AS Month, account_id AS accountBOM,dt
FROM UsefulFields
WHERE dt=LAST_DAY(dt,Month)
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, account_id AS accountEOM,dt
FROM UsefulFields
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
,Classification AS(
SELECT DISTINCT Account,Month
,CASE WHEN ActiveBOM=1 AND ActiveEOM=1 THEN "1.Mantain"
     WHEN ActiveBOM=1 AND ActiveEOM=0 THEN "2.Loss"
     WHEN ActiveBOM=0 AND ActiveEOM=1 THEN "3.Gain"
     ELSE "4.Null" END AS GainLossFlag
FROM CustomerStatus
)
,Churners AS(
SELECT DISTINCT Month,Account,GainLossFlag
 ,CASE WHEN GainLossFlag="2.Loss" THEN "1.Churner"
  ELSE "2.NonChurner" END AS ChurnFlag
FROM Classification
)
,GlennsFile AS(
SELECT DISTINCT DATE_TRUNC(PARSE_DATE("%Y%m%d",reporting_date_key),Month) AS Fecha,src_account_id,account_name,lob
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-17_Jamaica_VoluntaryChurners_v2`
WHERE lob="Mobile Postpaid"
GROUP BY 1,2,3,4
)
,VoluntaryChurners AS(
SELECT DISTINCT *
FROM Churners m INNER JOIN GlennsFile d ON m.account=d.src_account_id AND Fecha=Month
)
,ChurnTypeClassification AS(
SELECT c.Month,c.Account,v.src_account_id,c.ChurnFlag
,CASE WHEN c.ChurnFlag="1.Churner" AND v.src_account_id IS NULL THEN "1.Involuntary"
      WHEN c.ChurnFlag="1.Churner" AND  v.src_account_id IS NOT NULL THEN "2.Voluntary"
      ELSE NULL END AS ChurnType
FROM Churners c LEFT JOIN VoluntaryChurners v ON c.Account=v.src_account_id AND c.Month=v.Month
GROUP BY Month,c.Account,v.src_account_id,c.ChurnFlag
)
SELECT Month,ChurnFlag,ChurnType,COUNT(DISTINCT Account) AS Records
FROM ChurnTypeClassification 
GROUP BY Month,ChurnFlag,ChurnType
ORDER BY Month,ChurnFlag,ChurnType


