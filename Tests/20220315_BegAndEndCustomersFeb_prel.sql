WITH 

CustomersBegFebruary AS (
SELECT DISTINCT act_acct_cd, min(act_cust_strt_dt) AS MinStartDate,DATE_TRUNC(load_dt, MONTH) AS February
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt='2022-02-01'
AND (fi_outst_age <90 OR fi_outst_age is null)
GROUP BY act_acct_cd,February
)
,CustomersBegFebruaryCount AS (
SELECT DISTINCT COUNT(act_acct_cd) AS InitialAccounts,February
FROM CustomersBegFebruary 
WHERE MinStartDate<'2022-02-01'
GROUP BY February
)
, CustomersEndFebruary AS (
SELECT COUNT(DISTINCT act_acct_cd) AS EndingAccounts,DATE_TRUNC(load_dt, MONTH) AS February
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt='2022-02-21'
 AND (fi_outst_age <90 OR fi_outst_age is null)
GROUP BY February
)

SELECT InitialAccounts, EndingAccounts
FROM CustomersBegFebruaryCount b INNER JOIN CustomersEndFebruary e ON b.February=e.February
