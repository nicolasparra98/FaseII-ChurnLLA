WITH FixedConvergency AS(
 SELECT * 
 FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-05_CWP_Fixed_DashboardInput`
)

,MobileConvergency AS(
SELECT * 
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-05_CWP_Mobile_DashboardInput` 
)


,FullCustomerBase AS(
SELECT DISTINCT
CASE WHEN f.household_id=m.household_id THEN f.FixedMonth
      WHEN f.household_id IS NOT NULL AND m.household_id IS NULL THEN f.FixedMonth
      WHEN f.household_id IS NULL AND m.household_id IS NOT NULL THEN m.Mobile_Month
      WHEN f.household_id IS NULL AND f.FixedAccount IS NOT NULL THEN f.FixedMonth
      WHEN m.household_id IS NULL AND m.Mobile_Account IS NOT NULL THEN m.Mobile_Month
      END AS Month
,CASE WHEN f.household_id=m.household_id THEN f.FixedAccount
      WHEN f.household_id IS NOT NULL AND m.household_id IS NULL THEN f.FixedAccount
      WHEN f.household_id IS NULL AND m.household_id IS NOT NULL THEN safe_cast(m.Mobile_Account as int64)
      WHEN f.household_id IS NULL AND f.FixedAccount IS NOT NULL THEN f.FixedAccount
      WHEN m.household_id IS NULL AND m.Mobile_Account IS NOT NULL THEN safe_cast(m.Mobile_Account as int64)
      END AS FinalAccount,
      FixedAccount, Mobile_Account
,CASE WHEN (F_ActiveBOM =1 AND Mobile_ActiveBOM=1) or (F_ActiveBOM=1 AND (Mobile_ActiveBOM=0 or Mobile_ActiveBOM IS NULL)) or ((F_ActiveBOM=0 OR F_ActiveBOM IS NULL) AND Mobile_ActiveBOM=1) THEN 1
ELSE 0 END AS Final_BOM_ActiveFlag,
CASE WHEN (F_ActiveEOM =1 AND Mobile_ActiveEOM=1) or (F_ActiveEOM=1 AND (Mobile_ActiveEOM=0 or Mobile_ActiveEOM IS NULL)) or ((F_ActiveEOM=0 OR F_ActiveEOM IS NULL) AND Mobile_ActiveEOM=1) THEN 1
ELSE 0 END AS Final_EOM_ActiveFlag
,CASE WHEN f.FmcFlagFix=m.FmcFlagMob THEN f.FmcFlagFix
      WHEN (f.FmcFlagFix="4.Fixed Only" AND m.FmcFlagMob IS NULL) OR f.fmcFlagFix= "3. Hard Bundle" THEN f.fmcFlagFix
      WHEN fmcFlagFix IS NOT NULL and FmcFlagMob IS NULL THEN "4.Fixed Only"
      WHEN FmcFlagMob IS NOT NULL AND fmcFlagFix IS NULL THEN "4.Mobile Only"
      END AS FmcFlag
,f.* except (fixedmonth, household_id, FixedAccount), m.* except (mobile_month, household_id,Mobile_Account)
FROM FixedConvergency f FULL OUTER JOIN MobileConvergency m ON f.household_id=m.household_id and f.FixedMonth = m.Mobile_Month
),

FullCustomerBase_Flags AS
(
  SELECT *,
  CASE WHEN (B_FixedTenure IS NOT NULL AND B_MobileTenure IS NULL) THEN B_FixedTenure
  WHEN (B_FixedTenure = B_MobileTenure) THEN B_FixedTenure
  WHEN (B_MobileTenure IS NOT NULL AND B_FixedTenure IS NULL) THEN B_MobileTenure
  WHEN (B_FixedTenure <> B_MobileTenure AND (B_FixedTenure = "Late-Tenure"  or B_MobileTenure = "Late-Tenure" )) THEN "Late-Tenure"
  END AS B_Final_Tenure,
  CASE WHEN (E_FixedTenure IS NOT NULL AND E_MobileTenure IS NULL) THEN E_FixedTenure
  WHEN (E_FixedTenure = E_MobileTenure) THEN E_FixedTenure
  WHEN (E_MobileTenure IS NOT NULL AND E_FixedTenure IS NULL) THEN E_MobileTenure
  WHEN (E_FixedTenure <> E_MobileTenure AND (E_FixedTenure = "Late-Tenure"  or E_MobileTenure = "Late-Tenure" )) THEN "Late-Tenure"
  END AS E_Final_Tenure,
  CASE WHEN
  B_TechFlag IS NOT NULL THEN B_TechFlag
  ELSE "Wireless" END AS B_Final_TechFlag,
  CASE WHEN E_TechFlag IS NOT NULL THEN E_TechFlag
  ELSE "Wireless" END AS E_Final_TechFlag,
  CASE WHEN F_ActiveBOM=0 AND Mobile_ActiveBOM=0 THEN NULL
  WHEN FMCFlag = "4.Mobile Only" OR ((FMCFlag="1.Soft FMC" OR FMCFlag="2.Near FMC") AND (F_ActiveBOM=0 AND Mobile_ActiveBOM=1) ) THEN "MobileOnly"
  WHEN FMCFlag = "4.Fixed Only" and B_MixCode_Adj = "1P" THEN "Fixed 1P"
  WHEN FMCFlag = "4.Fixed Only" and B_MixCode_Adj = "2P" THEN "Fixed 2P"
  WHEN FMCFlag = "4.Fixed Only" and B_MixCode_Adj = "3P" THEN "Fixed 3P"
  WHEN (FMCFlag = "1.Soft FMC" OR FMCFlag = "3. Hard Bundle") THEN "Real FMC"
  WHEN FMCFlag = "2.Near FMC" THEN "Near FMC"
  END AS B_FMCType,
  CASE WHEN F_ActiveEOM=1 AND ChurnFlag="Churner" THEN "ChurnerActiveEOM"
  WHEN F_ActiveEOM=0 AND Mobile_ActiveEOM=0 THEN NULL
  WHEN FMCFlag = "4.Mobile Only" OR ((FMCFlag="1.Soft FMC" OR FMCFlag="2.Near FMC") AND (F_ActiveEOM=0 AND Mobile_ActiveEOM=1) ) THEN "MobileOnly"
  WHEN (FMCFlag = "1.Soft FMC" OR FMCFlag = "3. Hard Bundle") THEN "Real FMC"
  WHEN FMCFlag = "2.Near FMC" THEN "Near FMC"
  WHEN FMCFlag = "4.Fixed Only" and E_MixCode_Adj = "1P" THEN "Fixed 1P"
  WHEN FMCFlag = "4.Fixed Only" and E_MixCode_Adj = "2P" THEN "Fixed 2P"
  WHEN FMCFlag = "4.Fixed Only" and E_MixCode_Adj = "3P" THEN "Fixed 3P"
  END AS E_FMCType,
  CASE WHEN (FMCFlag = "1.Soft FMC" OR FMCFlag = "3. Hard Bundle" or FMCFlag = "2.Near FMC" ) AND (F_ActiveBOM = 1 AND Mobile_ActiveBOM = 1) AND  B_MixCode_Adj = "1P" THEN "P2"
  WHEN (FMCFlag = "1.Soft FMC" OR FMCFlag = "3. Hard Bundle" or FMCFlag = "2.Near FMC" ) AND (F_ActiveBOM = 1 AND Mobile_ActiveBOM = 1) AND  B_MixCode_Adj = "2P" THEN "P3"
  WHEN (FMCFlag = "1.Soft FMC" OR FMCFlag = "3. Hard Bundle" or FMCFlag = "2.Near FMC" ) AND (F_ActiveBOM = 1 AND Mobile_ActiveBOM = 1) AND  B_MixCode_Adj = "3P" THEN "P4"
  WHEN FMCFlag = "4.Fixed Only" THEN "P1 Fixed"
  WHEN FMCFlag = "4.Mobile Only" OR (F_ActiveBOM= 0 OR F_ActiveBOM IS NULL) AND Mobile_ActiveBOM= 1 THEN "P1 Mobile"
  END AS B_FMCSegment,
  CASE WHEN (FMCFlag = "1.Soft FMC" OR FMCFlag = "3. Hard Bundle" or FMCFlag = "2.Near FMC" ) AND (F_ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND  E_MixCode_Adj ="1P" AND ChurnFlag = "Non Churner" THEN "P2"
  WHEN (FMCFlag = "1.Soft FMC" OR FMCFlag = "3. Hard Bundle" or FMCFlag = "2.Near FMC" ) AND (F_ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND  E_MixCode_Adj = "2P" AND ChurnFlag = "Non Churner" THEN "P3"
  WHEN (FMCFlag = "1.Soft FMC" OR FMCFlag = "3. Hard Bundle" or FMCFlag = "2.Near FMC" ) AND (F_ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND  E_MixCode_Adj = "3P" AND ChurnFlag = "Non Churner" THEN "P4"
  WHEN FMCFlag = "4.Fixed Only" AND ChurnFlag = "Non Churner" THEN "P1 Fixed"
  WHEN FMCFlag = "4.Mobile Only" OR (F_ActiveEOM= 0 OR F_ActiveEOM IS NULL OR ChurnFlag = "Churner") AND Mobile_ActiveEOM= 1 THEN "P1 Mobile"
  END AS E_FMCSegment,
  (IFNULL(B_Fixed_MRC,0) + IFNULL(B_MobileMRC,0)) AS B_Total_MRC,
  (IFNULL(E_Fixed_MRC,0) + IFNULL(E_MobileMRC,0)) AS E_Total_MRC
  FROM FullCustomerBase
)
/*
SELECT DISTINCT --*
FinalAccount,FixedAccount,Mobile_Account,Final_BOM_ActiveFlag,Final_EOM_ActiveFlag,FmcFlag,F_ActiveBOM,F_ActiveEOM,Mobile_ActiveBOM,Mobile_ActiveEOM,Fix_B_Date,Fixed_B_Phone
,B_Overdue,Fixed_B_MaxStart,B_FixedTenure,B_MixCode_Adj,E_MixCode_Adj, B_TechFlag,MainMovement,SpinMovement,ChurnFlag,ChurnType,B_Final_TechFlag,E_Final_TechFlag,B_FMCType
,E_FMCType,B_FMCSegment,E_FMCSegment
FROM FullCustomerBase_Flags
WHERE Month = '2022-02-01' AND ChurnFlag="Churner" AND E_FMCType IS NOT NULL
AND E_FMCType<>"MobileOnly" 
--AND ChurnType="1. VoluntaryChurner"
--AND Final_EOM_ActiveFlag=1
--AND B_FMCSegment<>"P1 Fixed"
--churnflag=churner y ver sus segmentos de fmc y el type 
*/
SELECT DISTINCT * --Month,ChurnType,COUNT(DISTINCT FinalAccount)
FROM FullCustomerBase_Flags
WHERE Month = '2022-02-01'
--GROUP BY 1,2 ORDER BY 1,2
