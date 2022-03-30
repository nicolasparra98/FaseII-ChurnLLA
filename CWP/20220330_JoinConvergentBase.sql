WITH 
Convergente AS(
SELECT DISTINCT *,PARSE_DATE("%Y%m%d",Date) as Mes
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwc_base_convergente_enero_febrero` 
)
,DNA AS(
SELECT DISTINCT DATE_TRUNC(DT,Month) AS Month,ACT_ACCT_CD
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
)
SELECT DISTINCT * FROM Convergente c INNER JOIN DNA d ON c.SERVICE_id=d.act_acct_cd AND c.Mes=d.Month
