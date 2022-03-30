WITH 
Convergente AS(
SELECT DISTINCT *,PARSE_DATE("%Y%m%d",Date) as Mes
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwc_base_convergente_enero_febrero` 
)
,DNA AS(
SELECT DISTINCT DATE_TRUNC(DT,Month) AS Month,ACT_ACCT_CD,ACT_CONTACT_PHONE_3 AS CONTACTO
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
)
SELECT DISTINCT * FROM Convergente c INNER JOIN DNA d ON SAFE_CAST(c.HOUSEHOLD_id AS STRING)=d.CONTACTO AND c.Mes=d.Month
