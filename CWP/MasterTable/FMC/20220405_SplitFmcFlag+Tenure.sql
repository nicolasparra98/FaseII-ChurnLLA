WITH 
Convergente AS(
SELECT DISTINCT *,DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH) as Mes
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwc_base_convergente_enero_febrero` 
WHERE telefonia="Pospago" AND Unidad_de_negocio="1. B2C" 
 AND DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH)='2022-02-01'
)
,MobileUsefulFieldss AS(
SELECT SAFE_CAST(dt AS DATE) AS DT, DATE_TRUNC(SAFE_CAST(dt AS DATE),Month) AS MobileMonth
,LEFT(CONCAT(ACCOUNTNO,'000000000000') ,12) AS MobileAccount
,SAFE_CAST(SERVICENO AS INT64) AS SERVICENO
,MAX(SAFE_CAST(PARSE_DATETIME('%Y.%m.%d %H:%M:%S',STARTDATE_ACCOUNTNO) AS DATE)) AS MaxStart
,ACCOUNTNAME,NUMERO_IDENTIFICACION,SAFE_CAST(TOTAL_MRC_D AS FLOAT64) AS mrc_amt
,INV_PAYMT_DT
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history` 
WHERE BIZ_UNIT_D="B2C" AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN') AND INV_PAYMT_DT<>"nan"
and SAFE_CAST(dt AS DATE)=LAST_DAY(SAFE_CAST(dt AS DATE),Month)
GROUP BY DT,MobileMonth,MobileAccount
,SERVICENO,ACCOUNTNAME,NUMERO_IDENTIFICACION,mrc_amt,INV_PAYMT_DT
)
,MobileUsefulFields AS(
SELECT DISTINCT *
,CASE WHEN DATE_DIFF(DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(DT,MaxStart,DAY)>180 THEN "Late-Tenure" END AS MobileTenure
FROM MobileUsefulFieldss
)
,FixedUsefulFieldss AS(
SELECT DISTINCT DT,DATE_TRUNC(DATE_SUB(DT, INTERVAL 1 MONTH),Month) AS FixedMonth
,ACT_ACCT_CD AS FixedAccount,ACT_CONTACT_PHONE_3 AS CONTACTO
,FI_OUTST_AGE,MAX(SAFE_CAST(SAFE_CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart
,CASE WHEN (PD_BB_ACCS_MEDIA="FTTH" OR PD_TV_ACCS_MEDIA ="FTTH" OR PD_VO_ACCS_MEDIA="FTTH") THEN "FTTH"
      WHEN (PD_BB_ACCS_MEDIA="HFC" OR PD_TV_ACCS_MEDIA ="HFC" OR PD_VO_ACCS_MEDIA="HFC") THEN "HFC"
      WHEN (PD_BB_ACCS_MEDIA="VDSL" OR PD_TV_ACCS_MEDIA ="VDSL" OR PD_VO_ACCS_MEDIA="VDSL" OR 
            PD_BB_ACCS_MEDIA="COPPER" OR PD_TV_ACCS_MEDIA ="COPPER" OR PD_VO_ACCS_MEDIA="COPPER") THEN "COPPER"
      ELSE "Other" END AS TechFlag
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND pd_bb_prod_cd <> "" THEN 1 ELSE 0 END AS numBB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND pd_tv_prod_cd <> "" THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND pd_vo_prod_cd <> "" THEN 1 ELSE 0 END AS numVO
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
WHERE PD_MIX_CD<>"0P" AND (SAFE_CAST(FI_OUTST_AGE AS numeric)<=90 OR FI_OUTST_AGE IS NULL)
 AND dt='2022-03-02'
GROUP BY DT,FixedMonth,FixedAccount,CONTACTO,FI_OUTST_AGE,TechFlag,Numbb,NumTV,NumVO
)
,FixedUsefulFields AS(
SELECT DISTINCT *
,CASE WHEN DATE_DIFF(DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
      WHEN DATE_DIFF(DT,MaxStart,DAY)>180 THEN "Late-Tenure" END AS FixedTenure
FROM FixedUsefulFieldss
)
,HardBundleFlag AS(
SELECT DISTINCT ACT_ACCT_CD
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
WHERE PD_MIX_CD<>"0P"
AND ((PD_VO_PROD_CD = "1719" AND PD_BB_PROD_CD = "1743") OR 
(PD_VO_PROD_CD = "1719" AND PD_BB_PROD_CD = "1744") OR
(PD_VO_PROD_CD = "1718" AND PD_BB_PROD_CD = "1645"))
 AND (SAFE_CAST(FI_OUTST_AGE AS numeric)<=90 OR FI_OUTST_AGE IS NULL)
AND dt = '2022-03-02'
)
,Join_FMC AS(
SELECT DISTINCT *, d.FixedAccount AS Account
 ,CASE WHEN Tipo="1. Inscrito a Paquete completo" OR Tipo="2. Beneficio manual" THEN "1.Soft FMC"
       WHEN Tipo="2. Match_ID" OR Tipo="3. Contact number" THEN "2.Near FMC"
       ELSE "3.Fixed-HardBundle" END AS ConvFlag
 ,CASE WHEN DATE_DIFF(DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
       WHEN DATE_DIFF(DT,MaxStart,DAY)>180 THEN "Late-Tenure" END AS Tenure
FROM Convergente c RIGHT JOIN FixedUsefulFields d ON SAFE_CAST(c.household_id AS STRING)=SAFE_CAST(d.FixedAccount AS STRING)
  AND c.Mes=d.FixedMonth
)
,FixedConvergency AS(
SELECT DISTINCT *, j.FixedAccount AS FixedAccountt
 ,CASE WHEN ConvFlag="1.Soft FMC" OR ConvFlag="2.Near FMC" THEN ConvFlag
       WHEN ConvFlag="3.Fixed-HardBundle" AND h.ACT_ACCT_CD IS NOT NULL THEN "3. Hard Bundle"
       WHEN ConvFlag="3.Fixed-HardBundle" AND h.ACT_ACCT_CD IS NULL THEN "4.Fixed Only"
       END AS FmcFlagFix
FROM Join_FMC j LEFT JOIN HardBundleFlag h ON j.FixedAccount=h.ACT_ACCT_CD
)
,MobileConvergency AS(
SELECT DISTINCT *
 ,CASE WHEN Tipo="1. Inscrito a Paquete completo" OR Tipo="2. Beneficio manual" THEN "1.Soft FMC"
       WHEN Tipo="2. Match_ID" OR Tipo="3. Contact number" THEN "2.Near FMC"
       WHEN household_id IS NULL THEN "MobileOnly"
       ELSE "3.Fixed-HardBundle" END AS FmcFlagMob
FROM MobileUsefulFields m LEFT JOIN Convergente c ON m.SERVICENO=c.SERVICE_ID AND m.MobileMonth=c.Mes
)
,FinalConvergency AS(
SELECT DISTINCT fmcFlagFix,FmcFlagMob,f.household_id,m.household_id,f.FixedAccountt,m.MobileAccount
,CASE WHEN f.household_id=m.household_id THEN f.household_id
      WHEN f.household_id IS NOT NULL AND m.household_id IS NULL THEN f.household_id
      WHEN f.household_id IS NULL AND m.household_id IS NOT NULL THEN m.household_id
      WHEN f.household_id IS NULL AND f.FixedAccountt IS NOT NULL THEN f.FixedAccountt END AS FmcAccount
,CASE WHEN f.FmcFlagFix=m.FmcFlagMob THEN f.FmcFlagFix
      WHEN f.FmcFlagFix="3. Hard Bundle" AND m.FmcFlagMob IS NULL THEN f.fmcFlagFix
      WHEN (f.FmcFlagFix<>"4.Fixed Only" OR (fmcFlagMob IS NOT NULL AND FmcFlagFix="4.Fixed Only")) THEN "4.PartialConv"
      WHEN FmcFlagMob="MobileOnly" AND fmcFlagFix IS NULL THEN fmcflagmob
      ELSE "5.Fix/Mobile Only" END AS FmcFlag
FROM FixedConvergency f full JOIN MobileConvergency m ON f.household_id=m.household_id
)
,FullCustomerBase AS (
SELECT DISTINCT  
CASE WHEN (FixedAccount IS NOT NULL AND m.MobileAccount IS NOT NULL) OR (FixedAccount IS NOT NULL AND m.MobileAccount IS NULL) THEN FixedMonth
      WHEN (FixedAccount IS NULL AND m.MobileAccount IS NOT NULL) THEN MobileMonth
  END AS Month
,CASE WHEN (FixedAccount IS NOT NULL AND m.MobileAccount IS NOT NULL) OR (FixedAccount IS NOT NULL AND m.MobileAccount IS NULL) 
        THEN SAFE_CAST(FixedAccount AS STRING)
      WHEN (FixedAccount IS NULL AND m.MobileAccount IS NOT NULL) THEN m.MobileAccount
  END AS FinalAccount
,CASE WHEN FixedAccount IS NULL AND m.MobileAccount IS NOT NULL THEN "MobileOnly"
      WHEN FixedAccount IS NOT NULL AND m.MobileAccount IS NULL THEN "FixedOnly"
      WHEN m.MobileAccount IS NOT NULL AND SAFE_CAST(FixedAccount AS STRING)=m.MobileAccount THEN "FMC"
      ELSE "Other" END AS P1
,CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN "1P"
      WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN "2P"
      WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN "3P" END AS E_MixCode_Adj
, CASE WHEN (FixedTenure = "Early-Tenure" and MobileTenure = "Early-Tenure") OR (FixedTenure = "Early-Tenure" and MobileTenure IS NULL ) OR (FixedTenure IS NULL and MobileTenure = "Early-Tenure") Then "Early-Tenure"
WHEN (FixedTenure = "Late-Tenure" or MobileTenure = "Late-Tenure") THEN "Late-Tenure"
END AS TenureSegment
FROM FixedUsefulFields f FULL OUTER JOIN MobileConvergency m ON m.household_id=f.FixedAccount AND f.FixedMonth=m.MobileMonth
 --MobileUsefulFields m ON SAFE_CAST(f.FixedAccount AS STRING)=m.MobileAccount AND f.FixedMonth=m.MobileMonth
)
,FinalFlagFMC AS(
SELECT DISTINCT b.*,FmcFlag
,CASE WHEN FmcFlag="1.Soft FMC" OR FmcFlag="2.Near FMC" OR FmcFlag="3. Hard Bundle" THEN FmcFlag
      WHEN (FmcFlag="4.PartialConv" OR FmcFlag="5.Fix/Mobile Only") AND P1="FixedOnly" AND E_MixCode_Adj="1P" THEN "4.FixedOnly-1P"
      WHEN (FmcFlag="4.PartialConv" OR FmcFlag="5.Fix/Mobile Only") AND P1="FixedOnly" AND E_MixCode_Adj="2P" THEN "4.FixedOnly-2P"
      WHEN (FmcFlag="4.PartialConv" OR FmcFlag="5.Fix/Mobile Only") AND P1="FixedOnly" AND E_MixCode_Adj="3P" THEN "4.FixedOnly-3P"
      WHEN --P1="MobileOnly" AND 
      fmcflag="MobileOnly" THEN "5.MobileOnly"
      ELSE "INVESTIGATE" END AS FmcFinalFlag
,C.*
FROM FullCustomerBase b LEFT JOIN FinalConvergency c ON b.FinalAccount=SAFE_CAST(c.FmcAccount AS STRING)

)
--Prender para todo menos mobile only
/*
SELECT DISTINCT FmcFinalFlag,TenureSegment,COUNT(DISTINCT FinalAccount) 
FROM FinalFlagFMC
--WHERE FmcFinalFlag="INVESTIGATE"
GROUP BY FmcFinalFlag,TenureSegment
ORDER BY FmcFinalFlag,TenureSegment
*/

--Prender para Mobile only
/*
select distinct mobilemonth,fmcflagmob,MobileTenure,count(distinct serviceno)
from MobileConvergency 
group by mobilemonth,fmcflagmob,MobileTenure
order by mobilemonth,fmcflagmob,MobileTenure
*/
