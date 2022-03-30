WITH 
DerecognitionBase AS(
SELECT DISTINCT *
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwp_DRC_fijo_enero_febrero` 
WHERE ACT_ACCT_CD<>"ACT_ACCT_CD"
)
,DerecognitionBaseAdj AS(
SELECT DISTINCT SAFE_CAST(ACT_ACCT_CD AS INT64) AS ACT_ACCT_CD,PARSE_DATE("%Y%m%d",Date) as MonthDRC
FROM DerecognitionBase 
)
,DNAFixed AS(
SELECT DISTINCT DATE_TRUNC(dt,Month) AS Month,ACT_ACCT_CD,SAFE_CAST(FI_OUTST_AGE AS INT64) AS FI_OUTST_AGE
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
WHERE pd_mix_nm<>"0P"
)
SELECT DISTINCT Month,f.act_acct_cd
FROM DerecognitionBaseAdj d INNER JOIN DNAFixed f ON d.ACT_ACCT_CD=f.ACT_ACCT_CD AND d.MonthDRC=f.Month
WHERE FI_OUTST_AGE>90
