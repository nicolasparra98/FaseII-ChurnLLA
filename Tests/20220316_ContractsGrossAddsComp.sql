WITH FIRSTLASTDAYMONTH AS(
 SELECT DATE_TRUNC (LOAD_DT, MONTH) AS Month,MIN(LOAD_DT) AS FIRSTDAY, MAX(LOAD_DT) AS LASTDAY
 FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
 WHERE org_cntry = "Jamaica" AND (fi_outst_age < 90 OR fi_outst_age IS NULL) AND
 ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') 
 GROUP BY Month
),
RGUSFirstDay AS(
SELECT DISTINCT DATE_TRUNC(load_dt, MONTH) as Month, act_acct_cd, min(act_cust_strt_dt) as MinStartFirst,
CASE WHEN pd_mix_cd = "1P" THEN 1
WHEN pd_mix_cd = "2P" OR (pd_mix_cd = "3P" AND bundle_code ="GPON_HBO_GO") THEN 2
WHEN pd_mix_cd = "3P" AND (bundle_code <> "GPON_HBO_GO") THEN 3
ELSE NULL END AS RGUsFirst
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` t INNER JOIN 
FIRSTLASTDAYMONTH d on d.FIRSTDAY = t.LOAD_DT
WHERE org_cntry = "Jamaica" AND (fi_outst_age < 90 OR fi_outst_age IS NULL) AND
 ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
GROUP BY act_acct_cd, Month, RGUSFirst
),
RGUSLastDay AS(
SELECT DISTINCT DATE_TRUNC(load_dt, MONTH) as Month, act_acct_cd, min(act_cust_strt_dt) as MinStartLast,
CASE WHEN pd_mix_cd = "1P" THEN 1
WHEN pd_mix_cd = "2P" OR (pd_mix_cd = "3P" AND bundle_code ="GPON_HBO_GO") THEN 2
WHEN pd_mix_cd = "3P" AND (bundle_code <> "GPON_HBO_GO") THEN 3
ELSE NULL END AS RGUSLast
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` t INNER JOIN 
FIRSTLASTDAYMONTH d on d.LASTDAY = t.LOAD_DT
WHERE org_cntry = "Jamaica" AND (fi_outst_age < 90 OR fi_outst_age IS NULL) AND
 ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
GROUP BY act_acct_cd, Month, RGUSLast
),
CAMBIORGUS AS
(
    SELECT DISTINCT 
    --f.act_acct_cd, f.Month, 
    l.act_acct_cd, l.Month,
    CASE WHEN (RGUSLast - RGUSFirst) > 0 THEN "Gain"
    WHEN (RGUSLast - RGUSFirst) < 0 THEN "Loss"
    WHEN (RGUSLast - RGUSFirst) = 0 THEN "Maintain"
    WHEN (RGUSFirst > 0 AND RGUSLast IS NULL) THEN "Churner"
    WHEN (RGUSFirst IS NULL AND RGUSLast > 0 AND DATE_TRUNC (MinStartLast, MONTH) = '2022-02-01') THEN "New Customer"
    WHEN (RGUSFirst IS NULL AND RGUSLast > 0 AND DATE_TRUNC (MinStartLast, MONTH) <> '2022-02-01') THEN "Come Back to Life"
    WHEN RGUsFirst IS NULL AND RGUSLAST IS NULL THEN "Null"
    END AS CAMBIORGUSMONTH
    FROM RGUSFirstDay f 
    --LEFT JOIN RGUSLastDay l ON f.act_acct_cd = l.act_acct_cd and f.Month = l.Month
    RIGHT JOIN RGUSLastDay l ON f.act_acct_cd = l.act_acct_cd and f.Month = l.Month
)
, NuevosPau AS(
SELECT DISTINCT act_acct_cd
FROM CAMBIORGUS
WHERE CAMBIORGUSMONTH="New Customer"
)
, GrossAddsFebruary AS (
--Fecha empleada variable entre act_acct_inst_dt y act_cust_strt_dt
SELECT DISTINCT act_acct_cd, min(act_cust_strt_dt) AS MinStartDate
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
--load_dt: los que están activos el 28 de febrero
WHERE org_cntry="Jamaica" AND load_dt='2022-02-28'
 AND (fi_outst_age <90 OR fi_outst_age is null)
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
GROUP BY act_acct_cd
HAVING MinStartDate>='2022-02-01'
)
SELECT p.act_acct_cd AS ContratosPau, g.act_acct_cd AS ContratosGrossAdd
FROM NuevosPau p RIGHT JOIN GrossAddsFebruary g ON p.act_acct_cd=g.act_acct_cd
ORDER BY p.act_acct_cd


