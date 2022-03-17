WITH

GrossAddsMonth AS(
--Fecha empleada variable entre act_acct_inst_dt y act_cust_strt_dt
SELECT DISTINCT act_acct_cd, min(act_cust_strt_dt) AS MinStartDate,DATE_TRUNC(load_dt,MONTH) AS Month,pd_mix_cd
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
--load_dt: los que están activos el ultimo dia del mes
WHERE org_cntry="Jamaica" AND load_dt=LAST_DAY(load_dt,month)
 AND (fi_outst_age <90 OR fi_outst_age is null)
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
GROUP BY act_acct_cd,Month,pd_mix_cd
HAVING MinStartDate>=Month
)
,RGUsCount AS(
SELECT DISTINCT act_acct_cd,Month,pd_mix_cd,
CASE WHEN pd_mix_cd = "1P" THEN 1
     WHEN pd_mix_cd = "2P"  THEN 2
     WHEN pd_mix_cd = "3P" THEN 3 ELSE 1 END AS RGUS
FROM GrossAddsMonth 
GROUP BY act_acct_cd,Month,pd_mix_cd
)
SELECT DISTINCT RGUS,COUNT(DISTINCT act_acct_cd) AS Records,RGUS*COUNT(DISTINCT act_acct_cd) AS GrossAdds
FROM RGUsCount
GROUP BY RGUS
