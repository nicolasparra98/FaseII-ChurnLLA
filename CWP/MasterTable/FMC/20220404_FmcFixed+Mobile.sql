WITH 
Convergente AS(
SELECT DISTINCT *,DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH) as Mes
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwc_base_convergente_enero_febrero` 
WHERE telefonia="Pospago" AND Unidad_de_negocio="1. B2C" 
 AND DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH)='2022-02-01'
)
,MobileUsefulFields AS(
SELECT SAFE_CAST(dt AS DATE) AS DT, DATE_TRUNC(SAFE_CAST(dt AS DATE),Month) AS Month
,LEFT(CONCAT(ACCOUNTNO,'000000000000') ,12) AS ACT_ACCT_CD
,SAFE_CAST(SERVICENO AS INT64) AS SERVICENO
,MAX(SAFE_CAST(PARSE_DATETIME('%Y.%m.%d %H:%M:%S',STARTDATE_ACCOUNTNO) AS DATE)) AS MaxStart
,ACCOUNTNAME,NUMERO_IDENTIFICACION,SAFE_CAST(TOTAL_MRC_D AS FLOAT64) AS mrc_amt
,INV_PAYMT_DT
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history` 
WHERE BIZ_UNIT_D="B2C" AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN') AND INV_PAYMT_DT<>"nan"
GROUP BY DT,Month,ACT_ACCT_CD
,SERVICENO,ACCOUNTNAME,NUMERO_IDENTIFICACION,mrc_amt,INV_PAYMT_DT
)
,FixedUsefulFields AS(
SELECT DISTINCT DT,DATE_TRUNC(DATE_SUB(DT, INTERVAL 1 MONTH),Month) AS Month,ACT_ACCT_CD,ACT_CONTACT_PHONE_3 AS CONTACTO
,FI_OUTST_AGE,MAX(SAFE_CAST(SAFE_CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart
,CASE WHEN (PD_BB_ACCS_MEDIA="FTTH" OR PD_TV_ACCS_MEDIA ="FTTH" OR PD_VO_ACCS_MEDIA="FTTH") THEN "FTTH"
      WHEN (PD_BB_ACCS_MEDIA="HFC" OR PD_TV_ACCS_MEDIA ="HFC" OR PD_VO_ACCS_MEDIA="HFC") THEN "HFC"
      WHEN (PD_BB_ACCS_MEDIA="VDSL" OR PD_TV_ACCS_MEDIA ="VDSL" OR PD_VO_ACCS_MEDIA="VDSL" OR 
            PD_BB_ACCS_MEDIA="COPPER" OR PD_TV_ACCS_MEDIA ="COPPER" OR PD_VO_ACCS_MEDIA="COPPER") THEN "COPPER"
      ELSE "Other" END AS TechFlag
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
WHERE PD_MIX_CD<>"0P" AND (SAFE_CAST(FI_OUTST_AGE AS numeric)<=90 OR FI_OUTST_AGE IS NULL)
 AND dt='2022-03-02'
GROUP BY DT,Month,ACT_ACCT_CD,CONTACTO,FI_OUTST_AGE,TechFlag
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
SELECT DISTINCT *, d.ACT_ACCT_CD AS Account
 ,CASE WHEN Tipo="1. Inscrito a Paquete completo" OR Tipo="2. Beneficio manual" THEN "1.Soft FMC"
       WHEN Tipo="2. Match_ID" OR Tipo="3. Contact number" THEN "2.Near FMC"
       ELSE "3.Fixed-HardBundle" END AS ConvFlag
 ,CASE WHEN DATE_DIFF(DT,MaxStart,DAY)<=180 THEN "Early-Tenure"
       WHEN DATE_DIFF(DT,MaxStart,DAY)>180 THEN "Late-Tenure" END AS Tenure
FROM Convergente c RIGHT JOIN FixedUsefulFields d ON SAFE_CAST(c.household_id AS STRING)=SAFE_CAST(d.ACT_ACCT_CD AS STRING)
  AND c.Mes=d.Month
)
,FixedConvergency AS(
SELECT DISTINCT *, j.ACT_ACCT_CD AS ACC
 ,CASE WHEN ConvFlag="1.Soft FMC" OR ConvFlag="2.Near FMC" THEN ConvFlag
       WHEN ConvFlag="3.Fixed-HardBundle" AND h.ACT_ACCT_CD IS NOT NULL THEN "3. Hard Bundle"
       WHEN ConvFlag="3.Fixed-HardBundle" AND h.ACT_ACCT_CD IS NULL THEN "4.Fixed Only"
       END AS FmcFlagFix
FROM Join_FMC j LEFT JOIN HardBundleFlag h ON j.ACT_ACCT_CD=h.ACT_ACCT_CD
)
,MobileConvergency AS(
SELECT DISTINCT * 
 ,CASE WHEN Tipo="1. Inscrito a Paquete completo" OR Tipo="2. Beneficio manual" THEN "1.Soft FMC"
       WHEN Tipo="2. Match_ID" OR Tipo="3. Contact number" THEN "2.Near FMC"
       ELSE "3.Fixed-HardBundle" END AS FmcFlagMob
FROM MobileUsefulFields m INNER JOIN Convergente c ON m.SERVICENO=c.SERVICE_ID AND m.Month=c.Mes
)
,FinalConvergency AS(
SELECT DISTINCT fmcFlagFix,FmcFlagMob,f.household_id,m.household_id,f.ACC,m.ACT_ACCT_CD
,CASE WHEN f.household_id=m.household_id THEN f.household_id
      WHEN f.household_id IS NOT NULL AND m.household_id IS NULL THEN f.household_id
      WHEN f.household_id IS NULL AND m.household_id IS NOT NULL THEN m.household_id
      WHEN f.household_id IS NULL AND f.ACC IS NOT NULL THEN f.ACC END AS FmcAccount
,CASE WHEN f.FmcFlagFix=m.FmcFlagMob THEN f.FmcFlagFix
      WHEN f.FmcFlagFix="3. Hard Bundle" AND m.FmcFlagMob IS NULL THEN f.fmcFlagFix
      WHEN (f.FmcFlagFix<>"4.Fixed Only" OR (fmcFlagMob IS NOT NULL AND FmcFlagFix="4.Fixed Only")) THEN "4.PartialConv"
      ELSE "5.Fix/Mobile Only" END AS FmcFlag
FROM FixedConvergency f LEFT JOIN MobileConvergency m ON f.household_id=m.household_id
)
SELECT DISTINCT fmcFlag,COUNT(DISTINCT FmcAccount) AS Records
FROM finalConvergency 
--where fmcFlagFix<>"4.Fixed Only"
--WHERE FmcFlagMob = "3.Fixed-HardBundle"
GROUP BY fmcFlag
