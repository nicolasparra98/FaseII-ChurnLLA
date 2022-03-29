WITH
Fixed AS(
SELECT DISTINCT DATE_TRUNC(DT,Month) AS Month,ACT_ACCT_CD
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
)
,Voluntarios as(
SELECT DISTINCT DATE_TRUNC(PARSE_DATE("%Y%m%d",ClosingDate),Month) AS Fecha, COD_CUENTA
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220329_cwp_info_voluntary_churn_jan_feb` )

SELECT DISTINCT * 
FROM Fixed f INNER JOIN Voluntarios v ON f.ACT_ACCT_CD=v.COD_CUENTA AND Month=Fecha
