WITH 

FebruaryInstallations AS (
SELECT DISTINCT act_acct_cd, min(act_acct_inst_dt) AS MinStartDate
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" 
GROUP BY act_acct_cd
HAVING MinStartDate>'2022-02-01' OR MinStartDate='2022-02-01'
)

, ActiveCustomersEndFeb AS(
SELECT DISTINCT act_acct_cd
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt='2022-02-28'
GROUP BY act_acct_cd
)

, NewFebCustomers AS(
SELECT DISTINCT e.act_acct_cd
FROM FebruaryInstallations i INNER JOIN ActiveCustomersEndFeb e ON i.act_acct_cd=e.act_acct_cd
GROUP BY 1
)

SELECT COUNT(DISTINCT act_acct_cd) AS GrossAddsFeb
FROM NewFebCustomers
