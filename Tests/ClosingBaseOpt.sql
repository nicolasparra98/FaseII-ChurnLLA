 WITH 
 ClosingBase AS(
 SELECT act_acct_cd,DATE_TRUNC(dt, Month) AS Month,LAST_DAY(dt,month) AS LastDayMonth
 FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_fixed_historic_v2` 
 WHERE org_cntry = "Jamaica" AND dt=LAST_DAY(dt,month)
 AND (fi_outst_age <= 90 OR fi_outst_age IS NULL) 
 AND act_cust_typ_nm IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND act_acct_stat IN ('B','D','P','SN','SR','T','W')
 GROUP BY act_acct_cd,Month,LastDayMonth
 )
 SELECT Month,COUNT(DISTINCT act_acct_cd)
 FROM ClosingBase
 GROUP BY month
