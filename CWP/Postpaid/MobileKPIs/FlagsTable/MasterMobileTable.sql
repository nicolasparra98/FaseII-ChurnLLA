################################################# Mobile Customers ###############################################################################################
--CREATE OR REPLACE TABLE 
--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-05_CWP_Mobile_DashboardInput` AS

WITH 
Convergente AS(
SELECT DISTINCT *,DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH) as Mes
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwc_base_convergente_enero_febrero` 
WHERE telefonia="Pospago" AND Unidad_de_negocio="1. B2C" 
 AND DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH)='2022-02-01'
)
,MobileUsefulFields AS(
SELECT SAFE_CAST(dt AS DATE) AS DT, DATE_TRUNC(SAFE_CAST(dt AS DATE),Month) AS MobileMonth
,LEFT(CONCAT(ACCOUNTNO,'000000000000') ,12) AS MobileAccount
,SAFE_CAST(SERVICENO AS INT64) AS PhoneNumber
,MAX(SAFE_CAST(PARSE_DATETIME('%Y.%m.%d %H:%M:%S',STARTDATE_ACCOUNTNO) AS DATE)) AS MaxStart
,ACCOUNTNAME AS Mob_AccountName,NUMERO_IDENTIFICACION as Mobile_Id,SAFE_CAST(TOTAL_MRC_D AS FLOAT64) AS Mobile_MRC
,SAFE_CAST(PARSE_DATETIME('%Y.%m.%d %H:%M:%S',INV_EXP_DT)AS DATE)AS MobilePay_Dt
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history` 
WHERE BIZ_UNIT_D="B2C" AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN') 
--AND INV_EXP_DT<>"nan" 
GROUP BY DT,MobileMonth,MobileAccount
,SERVICENO,ACCOUNTNAME,NUMERO_IDENTIFICACION,Mobile_MRC,INV_EXP_DT,VOLUNTARY_FLG
)

,MobileActive_BOM AS(
SELECT DT AS B_Date,DATE_TRUNC(DATE_ADD(SAFE_CAST (dt AS DATE),INTERVAL 1 MONTH), MONTH) AS MobileMonth,
MobileAccount as MobileBOM, PhoneNumber as Phone_BOM, MaxStart as Mobile_B_MaxStart, Mob_AccountName as B_Mob_Acc_Name, Mobile_Id as B_Mobile_ID, Mobile_MRC as B_MobileMRC,
MobilePay_Dt as B_Mobile_Inv_Dt,DATE_DIFF(dt,MobilePay_dt,DAY) AS DueDays
,CASE WHEN DATE_DIFF(DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(DT, MaxStart,DAY)>180 THEN "Late-Tenure" END AS B_MobileTenure
FROM MobileUsefulFields
WHERE SAFE_CAST(dt AS DATE)='2022-02-03'
--LAST_DAY(SAFE_CAST(dt AS DATE),Month)
 --AND DATE_DIFF(dt,MobilePay_dt,DAY) <= 90
)


,MobileActive_EOM AS(
SELECT DT AS E_Date, DATE_TRUNC(SAFE_CAST (dt AS DATE), MONTH) AS MobileMonth,
MobileAccount as MobileEOM, PhoneNumber as Phone_EOM, MaxStart as Mobile_E_MaxStart, Mob_AccountName as E_Mob_Acc_Name, Mobile_Id as E_Mobile_ID, Mobile_MRC as E_MobileMRC,
MobilePay_Dt as E_Mobile_Inv_Dt,DATE_DIFF(dt,MobilePay_dt,DAY) AS DueDays
,CASE WHEN DATE_DIFF(DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(DT, MaxStart,DAY)>180 THEN "Late-Tenure" END AS E_MobileTenure
FROM MobileUsefulFields
WHERE SAFE_CAST(dt AS DATE)='2022-03-01'
--LAST_DAY(SAFE_CAST(dt AS DATE),Month)
 --AND DATE_DIFF(dt,MobilePay_dt,DAY) <= 90
)


,MobileCustomerStatus AS(
  SELECT DISTINCT 
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN b.MobileMonth
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN e.MobileMonth
  END AS Mobile_Month,
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN mobileBOM
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN mobileEOM
  END AS Mobile_Account,
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN Phone_BOM
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN Phone_EOM
  END AS PhoneNumber,
  CASE WHEN mobileBOM IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveBOM,
  CASE WHEN mobileEOM IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveEOM,
  b.* except (mobileBOM,MobileMonth), e.* except (mobileEOM, MobileMonth)
  FROM MobileActive_BOM b FULL OUTER JOIN MobileActive_EOM e on b.MobileBOM = e.MobileEOM and b.MobileMonth = e.MobileMonth
)
,MobileConvergency AS(
SELECT DISTINCT m.*, c.household_id
 ,CASE WHEN Tipo="1. Inscrito a Paquete completo" OR Tipo="2. Beneficio manual" THEN "1.Soft FMC"
       WHEN Tipo="2. Match_ID" OR Tipo="3. Contact number" THEN "2.Near FMC"
       WHEN household_id IS NULL THEN "4. MobileOnly"
       ELSE "3.Mobile-HardBundle" END AS FmcFlagMob
FROM MobileCustomerStatus m LEFT JOIN Convergente c ON m.PhoneNumber=c.SERVICE_ID AND m.Mobile_Month=c.Mes
)
,MainMovementBase AS(
SELECT DISTINCT m.*
,CASE WHEN Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1 THEN "1.Mantain"
      WHEN Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1 THEN "2.Gain"
      WHEN Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0 THEN "3.Loss"
      ELSE "4.Null" END AS MainMovement
FROM MobileConvergency m
)

##################Involuntary Churners###############################################

,FirstCustRecord AS(
SELECT DATE_TRUNC(dt, Month) AS FirstMonth, MobileAccount AS FirstAccount, dt AS FirstCustRecord
FROM MobileUsefulFields 
WHERE dt=DATE_TRUNC(dt,Month)
)
,LastCustRecord AS(
SELECT DATE_TRUNC(dt, Month) AS LastMonth, MobileAccount AS LastAccount, dt AS LastCustRecord
FROM MobileUsefulFields 
WHERE dt=LAST_DAY(dt,Month)
)
,NoOverdue AS(
SELECT DISTINCT DATE_TRUNC (dt, MONTH) AS NoOverdueMonth, MobileAccount AS NoOverdueAccount
FROM MobileUsefulFields m
 INNER JOIN FirstCustRecord r ON m.dt = r.FirstCustRecord and r.FirstAccount=m.MobileAccount
WHERE DATE_DIFF(dt,MobilePay_dt,DAY) <= 90
GROUP BY NoOverdueMonth, NoOverdueAccount
)
,OverdueLastDay AS(
SELECT DISTINCT DATE_TRUNC(dt, MONTH) AS OverdueMonth,MobileAccount AS OverdueAccount
,date_diff(dt, MaxStart, DAY) AS ChurnTenureDays
FROM MobileUsefulFields m
 INNER JOIN LastCustRecord r ON m.dt = r.LastCustRecord and r.lastaccount = m.MobileAccount
WHERE DATE_DIFF(dt,MobilePay_dt,DAY)>=90
ORDER BY OverdueMonth
)
,InvoluntaryNetChurners AS(
SELECT DISTINCT n.NoOverdueAccount AS InvAccount,n.NoOverdueMonth AS InvMonth,o.ChurnTenureDays
FROM NoOverdue n INNER JOIN OverdueLastDay o ON n.NoOverdueAccount=o.OverdueAccount AND n.NoOverdueMonth=o.OverdueMonth
)
,InvoluntaryChurners AS(
SELECT DISTINCT m.*,ChurnTenureDays
,CASE WHEN MainMovement="3.Loss" AND InvAccount IS NOT NULL THEN "2. Involuntary Churner" 
      WHEN MainMovement="3.Loss" AND InvAccount IS NULL THEN "1.VoluntaryChurner" END AS ChurnerType
FROM MainMovementBase m LEFT JOIN InvoluntaryNetChurners i ON m.Mobile_Account=i.InvAccount AND m.Mobile_Month=i.InvMonth
--GROUP BY 
)
--SELECT COUNT(DISTINCT INVACCOUNT) FROM InvoluntaryNetChurners
--/*
SELECT DISTINCT Mobile_Month,COUNT(DISTINCT Mobile_Account) --max(duedays)
FROM mobileCUSTOMERSTATUS
WHERE MOBILE_ACTIVEeOM=1
GROUP BY MOBILE_MONTH ORDER BY MOBILE_MONTH
--*/

