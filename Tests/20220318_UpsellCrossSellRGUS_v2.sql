WITH 

FirstDayRGU AS(
SELECT act_acct_cd, DATE_TRUNC(load_dt,Month) AS Month,load_dt,pd_mix_nm
 ,CASE WHEN pd_mix_cd = "1P" THEN 1
     WHEN pd_mix_cd = "2P"  THEN 2
     WHEN pd_mix_cd = "3P" THEN 3 ELSE NULL END AS RGUSFirst
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt=DATE_TRUNC(load_dt,Month)
 AND (fi_outst_age <90 OR fi_outst_age is null)
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)
,LastDayRGU AS(
SELECT act_acct_cd, DATE_TRUNC(load_dt,Month) AS Month,pd_mix_nm
 ,CASE WHEN pd_mix_cd = "1P" THEN 1
     WHEN pd_mix_cd = "2P"  THEN 2
     WHEN pd_mix_cd = "3P" THEN 3 ELSE NULL END AS RGUSLast
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt=LAST_DAY(load_dt,month)
 AND (fi_outst_age <90 OR fi_outst_age is null)
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)
,Classification AS(
SELECT l.act_acct_cd,l.Month,RGUSLast,RGUSFirst,f.pd_mix_nm AS FirstBundle,l.pd_mix_nm AS LastBundle
 ,CASE WHEN RGUSLast-RGUSFirst>0 THEN "Upsell"
     WHEN RGUSLast-RGUSFirst<0 THEN "Downsell"
     WHEN RGUSLast-RGUSFirst=0 THEN "Same"
     ELSE "Null" END AS SellFlag
FROM FirstDayRGU f INNER JOIN LastDayRGU l ON f.act_acct_cd=l.act_acct_cd
GROUP BY l.act_acct_cd,l.Month,RGUSLast,RGUSFirst,FirstBundle,LastBundle
)
SELECT Month,SellFlag,FirstBundle,LastBundle,COUNT(DISTINCT act_acct_cd) AS Records
FROM Classification
GROUP BY Month,SellFlag,FirstBundle,LastBundle
ORDER BY Month,SellFlag,FirstBundle,LastBundle
