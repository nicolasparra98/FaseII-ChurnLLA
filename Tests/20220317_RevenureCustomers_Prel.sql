WITH
LastDayMonth AS(
 SELECT DATE_TRUNC (LOAD_DT, MONTH) AS MONTH, MAX(LOAD_DT) AS LASTDAY, MIN(LOAD_DT) AS FIRSTDAY
 FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
 WHERE org_cntry = "Jamaica" 
 GROUP BY MONTH
)
,TotalClosingBase AS(
SELECT DISTINCT MONTH, LASTDAY, FIRSTDAY, act_acct_cd 
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` t
INNER JOIN LASTDAYMONTH l ON l.LASTDAY = t.load_dt AND DATE_TRUNC (t.LOAD_DT, MONTH) = l.MONTH
WHERE org_cntry = "Jamaica" AND (fi_outst_age < 90 OR fi_outst_age IS NULL) AND
ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
GROUP BY MONTH, act_acct_cd, LASTDAY, FIRSTDAY
)
,GrossAddsMonth AS (
SELECT DISTINCT act_acct_cd, min(act_cust_strt_dt) AS MinStartDate,DATE_TRUNC(load_dt,MONTH) AS Month,
fi_vo_mrc_amt,fi_bb_mrc_amt,fi_tv_mrc_amt,fi_tot_mrc_amt,
CASE WHEN length(CAST(act_acct_cd AS STRING))=12 THEN "Liberate"
     WHEN length(CAST(act_acct_cd AS STRING))=8  THEN "Cerillion" END AS System,
CASE WHEN fi_vo_mrc_amt IS NOT NULL THEN "VO" END AS VO,
CASE WHEN fi_bb_mrc_amt IS NOT NULL THEN "BB" END AS BB,
CASE WHEN fi_tv_mrc_amt IS NOT NULL THEN "TV" END AS TV
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt=LAST_DAY(load_dt,month)
 AND (fi_outst_age <90 OR fi_outst_age is null)
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
GROUP BY act_acct_cd,Month,fi_vo_mrc_amt,fi_bb_mrc_amt,fi_tv_mrc_amt,fi_tot_mrc_amt
--Fecha que se debe modificar dependiendo del mes
HAVING MinStartDate>='2022-02-01'
)
,GrossAddsClassification AS(
SELECT DISTINCT 
g.Month,System,VO,BB,TV,
CASE WHEN System="Liberate" AND VO="VO" THEN round(SUM(cast(fi_vo_mrc_amt as float64)),0) END AS MRCVOLIBERATE,
CASE WHEN System="Liberate" AND BB="BB" THEN round(SUM(cast(fi_bb_mrc_amt as float64)),0) END AS MRCBBLIBERATE,
CASE WHEN System="Liberate" AND TV="TV" THEN round(SUM(cast(fi_tv_mrc_amt as float64)),0) END AS MRCTVLIBERATE,
CASE WHEN System="Cerillion" THEN round(SUM(cast(fi_tot_mrc_amt as float64)),0) END AS MRCCerillion
FROM GrossAddsMonth g JOIN TotalClosingBase t on g.act_acct_cd=t.act_acct_cd
GROUP BY 1,2,3,4,5
)

SELECT Month,MRCVOLIBERATE, MRCBBLIBERATE,MRCCerillion,MRCTVLIBERATE
FROM GrossAddsClassification
GROUP BY 1,2,3,4,5
