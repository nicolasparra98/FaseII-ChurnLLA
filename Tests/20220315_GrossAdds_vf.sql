WITH

GrossAddsFebruary AS (
--Fecha empleada variable entre act_acct_inst_dt y act_cust_strt_dt
SELECT DISTINCT act_acct_cd, min(act_acct_inst_dt) AS MinStartDate
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
--load_dt: los que están activos el 28 de febrero
WHERE org_cntry="Jamaica" AND load_dt='2022-02-28'
 AND (fi_outst_age <90 OR fi_outst_age is null)
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
GROUP BY act_acct_cd
HAVING MinStartDate>='2022-02-01'
)

SELECT COUNT(DISTINCT act_acct_cd) AS GrossAddsFeb
FROM GrossAddsFebruary 
