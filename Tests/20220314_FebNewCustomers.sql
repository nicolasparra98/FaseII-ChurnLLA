WITH 

ActiveCustomersBegFeb AS(
SELECT DISTINCT cst_cust_cd
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt='2022-02-01'
GROUP BY cst_cust_cd
),

ActiveCustomersEndFeb AS(
SELECT DISTINCT cst_cust_cd, min(act_acct_inst_dt) AS MinInstDate
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt='2022-02-28'
GROUP BY cst_cust_cd
),

NewFebCustomers AS(
SELECT DISTINCT e.cst_cust_cd, e.MinInstDate, DATE_TRUNC(e.MinInstDate, MONTH) AS MonthInst
FROM ActiveCustomersBegFeb b RIGHT JOIN ActiveCustomersEndFeb e ON b.cst_cust_cd=e.cst_cust_cd
GROUP BY 1,2,3
HAVING DATE_TRUNC(MonthInst, MONTH)='2022-02-01'
)

SELECT COUNT(DISTINCT CST_CUST_CD) AS FebNewCustomers--*
FROM NewFebCustomers
--ORDER BY cst_cust_cd, mininstdate asc
