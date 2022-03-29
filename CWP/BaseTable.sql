WITH
UsefulFields AS(
SELECT ACT_ACCT_CD, DT
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
  WHERE ACT_CUST_TYP_NM="Residencial"
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(DT),Month) AS Month, ACT_ACCT_CD AS accountBOM,dt
FROM UsefulFields
WHERE dt=DATE_TRUNC(DT, Month)
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(DT),MONTH) AS Month, ACT_ACCT_CD AS accountEOM,dt
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

SELECT * FROM CustomerStatus
