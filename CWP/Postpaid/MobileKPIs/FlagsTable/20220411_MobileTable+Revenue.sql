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
,ACCOUNTNAME AS Mob_AccountName--,NUMERO_IDENTIFICACION as Mobile_Id
,SAFE_CAST(TOTAL_MRC_D AS FLOAT64) AS Mobile_MRC
,INV_PAYMT_DT as Mobile_Dt
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history` 
WHERE BIZ_UNIT_D="B2C" AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN') AND INV_PAYMT_DT<>"nan" 
GROUP BY DT,MobileMonth,MobileAccount
,SERVICENO,ACCOUNTNAME,NUMERO_IDENTIFICACION,Mobile_MRC,INV_PAYMT_DT
)
,MobileRGUsPerUser AS(
SELECT DISTINCT MobileMonth,dt AS RGUsdt,MobileAccount AS RgusAccount,count(distinct PhoneNumber) AS NumRGUs
FROM MobileUsefulFields
GROUP BY MobileMonth,dt,MobileAccount
)
,AverageMRC_Mobile AS(
  SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS MobileMonth, MobileAccount AS MobileMRCAccount, ROUND(avg(Mobile_MRC),2) AS AvgMRC
  FROM MobileUsefulFields 
  WHERE Mobile_MRC IS NOT NULL AND Mobile_MRC <>0.0
  GROUP BY MobileMonth, MobileAccount
)
,MobileActive_BOM AS(
SELECT DT AS B_Date, DATE_TRUNC(DATE_ADD(SAFE_CAST (dt AS DATE),INTERVAL 1 MONTH), MONTH) AS MobileMonth
,MobileAccount as MobileBOM, PhoneNumber as Phone_BOM, MaxStart as Mobile_B_MaxStart, Mob_AccountName as B_Mob_Acc_Name--, Mobile_Id as B_Mobile_ID
,NumRGUs as B_NumRGUs,Mobile_Dt as B_Mobile_Inv_Dt, Mobile_MRC as B_MobileMRC,AvgMRC AS B_Mobile_AvgMRC
,CASE WHEN DATE_DIFF(DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(DT, MaxStart,DAY)>180 THEN "Late-Tenure" END AS B_MobileTenure
FROM MobileUsefulFields m
 INNER JOIN MobileRGUsPerUser r ON  m.MobileAccount = r.RGUsAccount AND m.dt = r.RGUsdt
 LEFT JOIN AverageMRC_Mobile a ON m.MobileAccount = a.MobileMRCAccount AND m.MobileMonth = a.MobileMonth
WHERE SAFE_CAST(dt AS DATE)=LAST_DAY(SAFE_CAST(dt AS DATE),Month)
)
,MobileActive_EOM AS(
SELECT DT AS E_Date, DATE_TRUNC(SAFE_CAST (dt AS DATE), MONTH) AS MobileMonth
,MobileAccount as MobileEOM, PhoneNumber as Phone_EOM, MaxStart as Mobile_E_MaxStart, Mob_AccountName as E_Mob_Acc_Name--, Mobile_Id as E_Mobile_ID
,NumRGUs as E_NumRGUs,Mobile_Dt as E_Mobile_Inv_Dt, Mobile_MRC as E_MobileMRC, AvgMRC AS E_Mobile_AvgMRC
,CASE WHEN DATE_DIFF(DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(DT, MaxStart,DAY)>180 THEN "Late-Tenure" END AS E_MobileTenure
FROM MobileUsefulFields m
 INNER JOIN MobileRGUsPerUser r ON  m.MobileAccount = r.RGUsAccount AND m.dt = r.RGUsdt
 LEFT JOIN AverageMRC_Mobile a ON m.MobileAccount = a.MobileMRCAccount AND m.MobileMonth = a.MobileMonth
WHERE SAFE_CAST(dt AS DATE)=LAST_DAY(SAFE_CAST(dt AS DATE),Month)
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
,CASE WHEN (E_NumRGUs - B_NumRGUs) = 0 THEN "1.SameRGUs" 
      WHEN (E_NumRGUs - B_NumRGUs) > 0 THEN "2.Upsell"
      WHEN (E_NumRGUs - B_NumRGUs) < 0 THEN "3.Downsell"
      WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (Mobile_E_MaxStart, MONTH) = '2022-02-01') THEN "4.New Customer"
      WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (Mobile_E_MaxStart, MONTH) <> '2022-02-01') THEN "5.Come Back to Life"
      WHEN (B_NumRGUs > 0 AND E_NumRGUs IS NULL) THEN "6.Null last day"
      WHEN B_NumRGUs IS NULL AND E_NumRGUs IS NULL THEN "7.Always null"
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

SELECT DISTINCT *
-- EXCEPT(Phone_BOM,B_Mob_Acc_Name,Phone_EOM,E_Mob_Acc_Name,E_Mobile_Inv_Dt,B_MobileMRC,E_MobileMRC,Mobile_MRC_diff
--,Mobile_E_MaxStart,Mobile_B_MaxStart)
--Mobile_Month,MobileMainMovement,MobileSpinFlag
--,COUNT(DISTINCT Mobile_Account) AS Records
--,ROUND(SUM(E_MobileMRC-B_MobileMRC),2) AS Revenue
--,ROUND(SUM(E_MobileMRC-B_MobileMRC)/COUNT(DISTINCT Mobile_Account),2) AS ARPU
--, ROUND(SUM(DISTINCT E_Mobile_AvgMRC)-SUM(DISTINCT B_Mobile_AvgMRC),2) AS Revenue
--,ROUND((SUM(DISTINCT E_Mobile_AvgMRC)-SUM(DISTINCT B_Mobile_AvgMRC))/COUNT(DISTINCT Mobile_Account),2) AS ARPC
FROM MobileConvergency 
WHERE Mobile_Month='2022-02-01'
AND MobileMainMovement="3.Downsell"
--AND MobileSpinFlag="3.Downspin"
--GROUP BY Mobile_Month,MobileMainMovement,MobileSpinFlag
--ORDER BY Mobile_Month,MobileMainMovement,MobileSpinFlag
--order by mobile_mrc_diff desc


