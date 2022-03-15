WITH 

CustomersFirstDate AS(
SELECT DISTINCT cst_cust_cd, min(act_acct_inst_dt) AS MinInstDate
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt='2022-02-28'
GROUP BY cst_cust_cd
)

SELECT DATE_TRUNC(MinInstDate, MONTH) as MonthInst, COUNT(DISTINCT cst_cust_cd)
FROM CustomersFirstDate
GROUP BY MonthInst 
ORDER BY MonthInst
