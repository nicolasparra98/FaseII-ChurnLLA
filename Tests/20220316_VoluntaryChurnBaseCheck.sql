WITH 
BaseInvolAjustada AS(
SELECT  *,PARSE_DATE("%Y%m%d",reporting_date_key) AS Fecha,LEFT(CAST(src_account_id AS STRING),12) AS src_account_idAdj
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-16_Jamaica_VoluntaryChurners`
)
,
BaseJamaica AS(
SELECT act_acct_cd, cst_cust_cd,cst_cust_name, org_cntry
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
)

--Conteos para cada sistema
SELECT trading_name, COUNT(DISTINCT src_account_id) AS Registros
FROM BaseInvolAjustada
GROUP BY trading_name

--Cruce Liberate con DNA
/*SELECT COUNT(DISTINCT src_account_idAdj)
FROM BaseJamaica j INNER JOIN BaseInvolAjustada i ON j.cst_cust_name=i.account_name
WHERE trading_name="LIME"*/

--Cruce Cerilion con DNA
/*SELECT COUNT(DISTINCT src_account_id)
FROM BaseJamaica j INNER JOIN BaseInvolAjustada i ON j.act_acct_cd=i.src_account_id
WHERE trading_name="FLOW"*/

