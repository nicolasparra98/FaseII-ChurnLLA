--CREATE OR REPLACE TABLE 
--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-12_CWP_RejoinerPopulation` AS
WITH 
Convergente AS(
SELECT DISTINCT *,DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH) as Mes
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwc_base_convergente_enero_febrero` 
WHERE telefonia="Pospago" AND Unidad_de_negocio="1. B2C" 
 AND DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH)='2022-02-01'
)

,FixedUsefulFields AS(
SELECT DISTINCT DT,DATE_TRUNC(DATE_SUB(DT, INTERVAL 1 MONTH),Month) AS FixedMonth
,ACT_ACCT_CD AS FixedAccount,ACT_CONTACT_PHONE_3 AS CONTACTO
,FI_OUTST_AGE,MAX(SAFE_CAST(SAFE_CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart, FI_TOT_MRC_AMT AS Fixed_MRC
,CASE WHEN (PD_BB_ACCS_MEDIA="FTTH" OR PD_TV_ACCS_MEDIA ="FTTH" OR PD_VO_ACCS_MEDIA="FTTH") THEN "FTTH"
      WHEN (PD_BB_ACCS_MEDIA="HFC" OR PD_TV_ACCS_MEDIA ="HFC" OR PD_VO_ACCS_MEDIA="HFC") THEN "HFC"
      WHEN (PD_BB_ACCS_MEDIA="VDSL" OR PD_TV_ACCS_MEDIA ="VDSL" OR PD_VO_ACCS_MEDIA="VDSL" OR 
            PD_BB_ACCS_MEDIA="COPPER" OR PD_TV_ACCS_MEDIA ="COPPER" OR PD_VO_ACCS_MEDIA="COPPER") THEN "COPPER"
      ELSE "Other" END AS TechFlag
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND pd_bb_prod_cd <> "" THEN 1 ELSE 0 END AS numBB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND pd_tv_prod_cd <> "" THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND pd_vo_prod_cd <> "" THEN 1 ELSE 0 END AS numVO,
PD_BB_PROD_CD, pd_tv_prod_cd, PD_VO_PROD_CD, pd_mix_nm
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
WHERE PD_MIX_CD<>"0P" 
GROUP BY DT,FixedMonth,FixedAccount,CONTACTO,FI_OUTST_AGE,TechFlag, Fixed_MRC, NumBB, NumTV, NumVO,
PD_BB_PROD_CD, PD_TV_PROD_CD, PD_VO_PROD_CD, PD_MIX_NM
)
,HardBundleFlag AS(
SELECT DISTINCT DT, ACT_ACCT_CD
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
WHERE PD_MIX_CD<>"0P"
AND ((PD_VO_PROD_CD = "1719" AND PD_BB_PROD_CD = "1743") OR 
(PD_VO_PROD_CD = "1719" AND PD_BB_PROD_CD = "1744") OR
(PD_VO_PROD_CD = "1718" AND PD_BB_PROD_CD = "1645"))
)
,AverageMRC_User AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, FixedAccount AS MrcAccount, round(avg(Fixed_MRC),0)  AS AvgMRC
FROM FixedUsefulFields
WHERE Fixed_mrc IS NOT NULL AND Fixed_mrc <> 0
GROUP BY Month, FixedAccount
)
,FixedActive_BOM AS(
SELECT f.DT as Fix_B_Date, DATE_TRUNC(DATE_ADD(SAFE_CAST (f.dt AS DATE),INTERVAL 1 MONTH), MONTH) AS FixedMonth
--DATE_TRUNC((SAFE_CAST (f.dt AS DATE)), MONTH) AS FixedMonth,
,FixedAccount as FixedAccount_BOM, Contacto AS Fixed_B_Phone, FI_OUTST_AGE as B_Overdue, MaxStart as Fixed_B_MaxStart
,CASE WHEN DATE_DIFF(f.DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(f.DT, MaxStart,DAY)>180 THEN "Late-Tenure" END AS B_FixedTenure
,TechFlag as B_TechFlag, (numBB+numTV+numVO) as B_NumRGUs
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN "BO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN "TV"
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN "VO"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN "BO+TV"
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN "BO+VO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN "VO+TV"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN "BO+VO+TV"
    END AS B_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN "1P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN "2P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN "3P" END AS B_MixCode_Adj
    ,PD_BB_PROD_CD AS B_bbCode, PD_TV_PROD_CD AS B_tvCode, PD_VO_PROD_CD AS B_voCode,Fixed_mrc AS B_Fixed_Mrc,AvgMRC AS B_Fixed_AvgMrc
    ,CASE WHEN h.ACT_ACCT_CD IS NOT NULL THEN "Hard FMC"
    ELSE "TBD" END AS B_Hard_FMC_Flag
    FROM FixedUsefulFields f LEFT JOIN HardBundleFlag h ON f.fixedaccount = h.ACT_ACCT_CD AND f.DT = h.DT
    LEFT JOIN AverageMRC_User a ON f.FixedAccount = a.MrcAccount AND f.FixedMonth = a.Month
    WHERE f.dt= last_day(f.dt,Month) --'2022-02-02' 
    AND (SAFE_CAST(FI_OUTST_AGE AS numeric)<=90 OR FI_OUTST_AGE IS NULL)
)

,FixedActive_EOM AS(
SELECT f.DT as Fix_E_Date,DATE_TRUNC((SAFE_CAST (f.dt AS DATE)), MONTH) AS FixedMonth,
--, DATE_TRUNC(DATE_SUB(SAFE_CAST (f.dt AS DATE),INTERVAL 1 MONTH), MONTH) AS FixedMonth,
FixedAccount as FixedAccount_EOM, Contacto AS Fixed_E_Phone, FI_OUTST_AGE as E_Overdue, MaxStart as Fixed_E_MaxStart
,CASE WHEN DATE_DIFF(f.DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(f.DT, MaxStart,DAY)>180 THEN "Late-Tenure" END AS E_FixedTenure
,TechFlag as E_TechFlag, (numBB+numTV+numVO) as E_NumRGUs
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN "BO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN "TV"
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN "VO"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN "BO+TV"
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN "BO+VO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN "VO+TV"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN "BO+VO+TV"
    END AS E_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN "1P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN "2P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN "3P" END AS E_MixCode_Adj
    ,PD_BB_PROD_CD AS E_bbCode, PD_TV_PROD_CD AS E_tvCode, PD_VO_PROD_CD AS E_voCode,Fixed_mrc AS E_Fixed_Mrc,AvgMRC AS E_Fixed_AvgMrc
    ,CASE WHEN h.ACT_ACCT_CD IS NOT NULL THEN "Hard FMC"
    ELSE "TBD" END AS E_Hard_FMC_Flag
    FROM FixedUsefulFields f LEFT JOIN HardBundleFlag h ON f.fixedaccount = h.ACT_ACCT_CD AND f.DT = h.DT
    LEFT JOIN AverageMRC_User a ON f.FixedAccount = a.MrcAccount AND f.FixedMonth = a.Month
    WHERE f.dt= last_day(f.dt,Month) --'2022-03-02' 
     AND (SAFE_CAST(FI_OUTST_AGE AS numeric)<=90 OR FI_OUTST_AGE IS NULL)
)

,CustomerStatus AS(
  SELECT DISTINCT
  CASE WHEN (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NOT NULL) OR (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NULL) THEN b.FixedMonth
      WHEN (FixedAccount_BOM IS NULL AND FixedAccount_EOM IS NOT NULL) THEN e.FixedMonth
  END AS FixedMonth,
      CASE WHEN (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NOT NULL) OR (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NULL) THEN FixedAccount_BOM
      WHEN (FixedAccount_BOM IS NULL AND FixedAccount_EOM IS NOT NULL) THEN FixedAccount_EOM
  END AS FixedAccount
  ,CASE WHEN FixedAccount_BOM IS NOT NULL THEN 1 ELSE 0 END AS F_ActiveBOM
  ,CASE WHEN FixedAccount_EOM IS NOT NULL THEN 1 ELSE 0 END AS F_ActiveEOM,
  b.* except (FixedAccount_BOM,FixedMonth), e.* except (FixedAccount_EOM, FixedMonth)
  ,(E_Fixed_MRC - B_Fixed_MRC) as MRCDiff
  FROM FixedActive_BOM b FULL OUTER JOIN FixedActive_EOM e
  ON b.FixedAccount_BOM = e.FixedAccount_EOM AND b.FixedMonth = e.FixedMonth
)
################ Movement Flags ################################################
,MainMovementBase AS(
SELECT a.*
,CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 THEN "1.SameRGUs" 
WHEN (E_NumRGUs - B_NumRGUs) > 0 THEN "2.Upsell"
WHEN (E_NumRGUs - B_NumRGUs) < 0 THEN "3.Downsell"
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (Fixed_E_MaxStart, MONTH) = '2022-02-01') THEN "4.New Customer"
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (Fixed_E_MaxStart, MONTH) <> '2022-02-01') THEN "5.Come Back to Life"
WHEN (B_NumRGUs > 0 AND E_NumRGUs IS NULL) THEN "6.Null last day"
WHEN B_NumRGUs IS NULL AND E_NumRGUs IS NULL THEN "7.Always null"
END AS MainMovement
FROM CustomerStatus a
)
,SpinMovementBase AS(
SELECT b.*,
CASE 
WHEN MainMovement = "1.SameRGUs" AND (E_Fixed_MRC - B_Fixed_MRC) > 0 THEN "1. Up-spin"
WHEN MainMovement = "1.SameRGUs" AND (E_Fixed_MRC - B_Fixed_MRC) < 0 THEN "2. Down-spin"
ELSE "3. No Spin" END AS SpinMovement
FROM MainMovementBase b
)
,InactiveUsers AS (
SELECT DISTINCT FixedMonth AS ExitMonth, fixedAccount,DATE_ADD(FixedMonth, INTERVAL 4 MONTH) AS RejoinerMonth
FROM CustomerStatus
WHERE F_ActiveBOM=1 AND F_ActiveEOM=0
)
,RejoinerPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.FixedAccount IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
,CASE WHEN RejoinerMonth>='2022-02-01' AND RejoinerMonth<=DATE_ADD('2022-02-01',INTERVAL 4 MONTH) THEN 1 ELSE 0 END AS PRFeb
FROM SpinMovementBase f LEFT JOIN InactiveUsers i ON f.FixedAccount=i.FixedAccount AND fixedMonth=ExitMonth
)
--/*
SELECT DISTINCT --FixedMonth,
PRFeb,COUNT(DISTINCT FixedAccount)
FROM RejoinerPopulation
WHERE RejoinerPopFlag=1
AND FixedMonth<>'2022-02-01'
GROUP BY 1--,2--,3
ORDER BY 1--,2
--*/
/*
SELECT DISTINCT *
FROM RejoinerPopulation 
WHERE FixedAccount=319003540000
--298087880000 solo tiene registros en feb y mar
ORDER BY FixedMonth
*/


