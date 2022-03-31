WITH 
MobileBase AS(
SELECT DISTINCT DATE_TRUNC(SAFE_CAST(dt AS DATE),MONTH) AS MONTH
,LEFT(CONCAT(ACCOUNTNO,'000000000000') ,12) AS ACCOUNTNO,SAFE_CAST(SERVICENO AS INT64) AS SERVICENO, ACCOUNTNAME
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history` 
WHERE BIZ_UNIT_D="B2C"
)
,
FlagsFMC AS(
SELECT DISTINCT DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH) as Mes
,HOUSEHOLD_ID,SERVICE_ID,Tipo
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwc_base_convergente_enero_febrero`
WHERE Telefonia="Pospago" AND Unidad_de_negocio="1. B2C"
)
,DNA AS(
SELECT DISTINCT DATE_TRUNC(DT,Month) AS Month
,SAFE_CAST(ACT_ACCT_CD AS STRING) AS ACT_ACCT_CD,ACT_ACCT_NAME
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
WHERE PD_MIX_CD<>"0P" 
)
SELECT *
--,COUNT(DISTINCT ACCOUNTNO)
FROM MobileBase m INNER JOIN FlagsFMC f ON m.SERVICENO=f.SERVICE_ID AND Month=Mes
 INNER JOIN DNA d ON m.ACCOUNTNO=d.ACT_ACCT_CD AND d.Month=m.Month
--GROUP BY Month,Tipo
--ORDER BY Month,Tipo
