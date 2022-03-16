WITH

GrossAddsFebruary AS (
SELECT DISTINCT act_acct_cd, min(act_cust_strt_dt) AS MinStartDate
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
--load_dt: los que están activos el 28 de febrero
WHERE org_cntry="Jamaica" AND load_dt='2022-02-28'
 AND (fi_outst_age <90 OR fi_outst_age is null)
GROUP BY act_acct_cd
HAVING MinStartDate>='2022-02-01'
)

SELECT COUNT(DISTINCT act_acct_cd) AS GrossAddsFeb
FROM GrossAddsFebruary 
