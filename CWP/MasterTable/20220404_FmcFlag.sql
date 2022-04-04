WITH 
Convergente AS(
SELECT DISTINCT *,DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH) as Mes
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwc_base_convergente_enero_febrero` 
WHERE telefonia="Pospago" AND Unidad_de_negocio="1. B2C" 
 AND DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH)='2022-02-01'
)
,DNA AS(
SELECT DISTINCT DT,DATE_TRUNC(DATE_SUB(DT, INTERVAL 1 MONTH),Month) AS Month,ACT_ACCT_CD,ACT_CONTACT_PHONE_3 AS CONTACTO
,FI_OUTST_AGE,MAX(SAFE_CAST(SAFE_CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart
/*,CASE WHEN (PD_VO_PROD_CD = "1719" AND PD_BB_PROD_CD = "1743") OR 
(PD_VO_PROD_CD = "1719" AND PD_BB_PROD_CD = "1744") OR
(PD_VO_PROD_CD = "1718" AND PD_BB_PROD_CD = "1645") THEN "3.Hard Bundle"
 ELSE Null END AS ConvergencyFlag1*/
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
FROM Convergente c RIGHT JOIN DNA d ON SAFE_CAST(c.household_id AS STRING)=SAFE_CAST(d.ACT_ACCT_CD AS STRING)
  AND c.Mes=d.Month
 --LEFT JOIN HardBundleFlag h ON d.ACT_ACCT_CD=h.ACT_ACCT_CD
)
,FinalJoinFMC AS(
SELECT DISTINCT *, j.ACT_ACCT_CD AS ACC
 ,CASE WHEN ConvFlag="1.Soft FMC" OR ConvFlag="2.Near FMC" THEN ConvFlag
       WHEN ConvFlag="3.Fixed-HardBundle" AND h.ACT_ACCT_CD IS NOT NULL THEN "3. Hard Bundle"
       WHEN ConvFlag="3.Fixed-HardBundle" AND h.ACT_ACCT_CD IS NULL THEN "4.Fixed Only"
       END AS FmcFlag
FROM Join_FMC j LEFT JOIN HardBundleFlag h ON j.ACT_ACCT_CD=h.ACT_ACCT_CD
)
/*SELECT *
FROM HardBundleFlag h LEFT JOIN FinalJoinFMC f ON f.ACC=h.ACT_ACCT_CD
WHERE f.ACC IS NULL*/

--/*
SELECT DISTINCT FmcFlag--,Tenure
,TechFlag
,COUNT(DISTINCT Account) AS Records
FROM FinalJoinFMC 
GROUP BY FmcFlag--,Tenure
,TechFlag
ORDER BY FmcFlag
,TechFlag
--*/
