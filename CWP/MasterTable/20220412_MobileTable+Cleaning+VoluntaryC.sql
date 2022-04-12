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
,ACCOUNTNO AS MobileAccount
,SAFE_CAST(SERVICENO AS INT64) AS PhoneNumber
,MAX(SAFE_CAST(PARSE_DATETIME('%Y.%m.%d %H:%M:%S',STARTDATE_ACCOUNTNO) AS DATE)) AS MaxStart
,ACCOUNTNAME AS Mob_AccountName,NUMERO_IDENTIFICACION as Mobile_Id
,SAFE_CAST(TOTAL_MRC_D AS FLOAT64) AS Mobile_MRC
,SAFE_CAST(PARSE_DATETIME('%Y.%m.%d %H:%M:%S',INV_EXP_DT)AS DATE)AS MobilePay_Dt
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history` 
WHERE BIZ_UNIT_D="B2C" AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN', 'RESTRICTED') AND INV_EXP_DT<>"nan" 
GROUP BY DT,MobileMonth,MobileAccount
,SERVICENO,ACCOUNTNAME,NUMERO_IDENTIFICACION,Mobile_MRC, MobilePay_Dt--, INV_PAYMT_DT,
)

,NumberRGUsPerUser AS(
SELECT DISTINCT MobileMonth,dt,MobileAccount,count(distinct Mobile_Id) AS NumRGUs
FROM MobileUsefulFields
GROUP BY MobileMonth,dt,MobileAccount
)
,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, MobileAccount, Round(avg(Mobile_MRC),2) AS AvgMRC_Mobile
  FROM MobileUsefulFields 
  WHERE Mobile_MRC IS NOT NULL AND Mobile_MRC <> 0
  GROUP BY Month, MobileAccount
)

,MobileActive_BOM AS(
SELECT m.DT AS B_Date, DATE_TRUNC(DATE_ADD(SAFE_CAST (m.dt AS DATE),INTERVAL 1 MONTH), MONTH) AS MobileMonth,
-- DATE_TRUNC(DATE_ADD(SAFE_CAST (dt AS DATE),INTERVAL 1 MONTH), MONTH) AS MobileMonth,
m.MobileAccount as MobileBOM, PhoneNumber as Phone_BOM, MaxStart as Mobile_B_MaxStart
, Mob_AccountName as B_Mob_Acc_Name, Mobile_Id as B_Mobile_ID
, Mobile_MRC as B_MobileMRC
, NumRGUs AS B_MobileRGUs, AvgMRC_Mobile as B_AvgMobileMRC
,CASE WHEN DATE_DIFF(m.DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(m.DT, MaxStart,DAY)>180 THEN "Late-Tenure" END AS B_MobileTenure
FROM MobileUsefulFields m INNER JOIN NumberRGUsPerUser r ON m.MobileAccount = r.MobileAccount AND m.dt = r.dt
LEFT JOIN AverageMRC_User a ON m.MobileAccount = a.MobileAccount AND  m.MobileMonth = a.Month
WHERE SAFE_CAST(m.dt AS DATE)= LAST_DAY(SAFE_CAST(m.dt AS DATE),Month)
--
--'2022-02-01' 
)


,MobileActive_EOM AS(
SELECT m.DT AS E_Date, DATE_TRUNC(SAFE_CAST (m.dt AS DATE), MONTH) AS MobileMonth,
m.MobileAccount as MobileEOM, PhoneNumber as Phone_EOM, MaxStart as Mobile_E_MaxStart
, Mob_AccountName as E_Mob_Acc_Name, Mobile_Id as E_Mobile_ID
, Mobile_MRC as E_MobileMRC
, NumRGUs AS E_MobileRGUs, AvgMRC_Mobile as E_AvgMobileMRC
,CASE WHEN DATE_DIFF(m.DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(m.DT, MaxStart,DAY)>180 THEN "Late-Tenure" END AS E_MobileTenure
FROM MobileUsefulFields m INNER JOIN NumberRGUsPerUser r ON m.MobileAccount = r.MobileAccount AND m.dt = r.dt
LEFT JOIN AverageMRC_User a ON m.MobileAccount = a.MobileAccount AND  m.MobileMonth = a.Month
WHERE SAFE_CAST(m.dt AS DATE)=  LAST_DAY(SAFE_CAST(m.dt AS DATE),Month) 
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

,MainMovementBase AS(
SELECT DISTINCT *
,CASE WHEN (E_MobileRGUs - B_MobileRGUs) = 0 THEN "1.SameRGUs" 
      WHEN (E_MobileRGUs - B_MobileRGUs) > 0 THEN "2.Upsell"
      WHEN (E_MobileRGUs - B_MobileRGUs) < 0 THEN "3.Downsell"
      WHEN (B_MobileRGUs IS NULL AND E_MobileRGUs > 0 AND DATE_TRUNC (Mobile_E_MaxStart, MONTH) = '2022-02-01') THEN "4.New Customer"
      WHEN (B_MobileRGUs IS NULL AND E_MobileRGUs > 0 AND DATE_TRUNC (Mobile_E_MaxStart, MONTH) <> '2022-02-01') THEN "5.Come Back to Life"
      WHEN (B_MobileRGUs > 0 AND E_MobileRGUs IS NULL) THEN "6.Null last day"
      WHEN B_MobileRGUs IS NULL AND E_MobileRGUs IS NULL THEN "7.Always null"
 END AS MobileMainMovement
FROM MobileCustomerStatus
)
,SpinClass AS(
SELECT DISTINCT *, ROUND((E_MobileMRC - B_MobileMRC),2) AS Mobile_MRC_Diff,
      CASE WHEN MobileMainMovement ="1.SameRGUs" AND (E_MobileMRC - B_MobileMRC)=0 THEN "1.Same"
      WHEN MobileMainMovement ="1.SameRGUs" AND (E_MobileMRC - B_MobileMRC)>0 THEN "2.Upspin"
      WHEN MobileMainMovement ="1.SameRGUs" AND (E_MobileMRC - B_MobileMRC)<0 THEN "3.Downspin"
      ELSE "4.NoSpin" END AS MobileSpinFlag
FROM MainMovementBase 
)

,MobileConvergency AS(
SELECT DISTINCT m.*, c.household_id
 ,CASE WHEN Tipo="1. Inscrito a Paquete completo" OR Tipo="2. Beneficio manual" THEN "1.Soft FMC"
       WHEN Tipo="2. Match_ID" OR Tipo="3. Contact number" THEN "2.Near FMC"
       WHEN household_id IS NULL THEN "4. MobileOnly"
       ELSE "3.Mobile-HardBundle" END AS FmcFlagMob
FROM SpinClass m LEFT JOIN Convergente c ON m.PhoneNumber=c.SERVICE_ID AND m.Mobile_Month=c.Mes
)
,DerecognitionBase AS(
SELECT '2022-02-01' AS DRCMonth, SAFE_CAST(ACCOUNTNO AS STRING) AS AccountNo
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-12_CWP_DerecognitionFebruary` 
)
,FebruaryCleaning AS(
SELECT m.*
,CASE WHEN d.AccountNo IS NOT NULL THEN 1 ELSE 0 END AS DRC
FROM MobileConvergency m LEFT JOIN DerecognitionBase d ON m.Mobile_Account=d.AccountNo
)
,VoluntaryChurners AS(
SELECT f.*
,CASE WHEN MobileMainMovement="6.Null last day" THEN "1.VoluntaryChurner" END AS ChurnerType
FROM FebruaryCleaning f
)
SELECT Mobile_Month,ChurnerType,COUNT(DISTINCT Mobile_Account)
FROM VoluntaryChurners 
GROUP BY 1,2 ORDER BY 1
/*
SELECT distinct Mobile_Month,DRC,COUNT(DISTINCT Mobile_Account)
FROM VoluntaryChurners
WHERE Mobile_month = '2022-02-01' 
AND Mobile_ActiveBOM=1
group by 1,2
order by 1,2
*/

