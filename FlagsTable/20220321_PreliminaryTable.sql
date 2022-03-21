WITH 

UsefulFields AS (
--Select all the columns that are going to be used from de DNA
SELECT act_acct_cd
,DATE_TRUNC(load_dt,MONTH) AS Month,load_dt
,pd_mix_cd, pd_bb_prod_nm
,fi_tot_mrc_amt, fi_outst_age
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)
,FirstDayRGU AS(
SELECT DISTINCT act_acct_cd,SAFE_CAST(fi_tot_mrc_amt AS FLOAT64) AS Bill_BOM
,CASE WHEN pd_mix_cd = "1P" THEN 1
     WHEN pd_mix_cd = "2P"  THEN 2
     WHEN pd_mix_cd = "3P" THEN 3 ELSE NULL END AS RGUS_BOM
FROM UsefulFields 
WHERE load_dt=DATE_TRUNC(load_dt,Month) AND (fi_outst_age<=90 OR fi_outst_age is null)
)
,LastDayRGU AS(
SELECT DISTINCT act_acct_cd,SAFE_CAST(fi_tot_mrc_amt AS FLOAT64) AS Bill_EOM,load_dt
,CASE WHEN pd_mix_cd = "1P" THEN 1
     WHEN pd_mix_cd = "2P"  THEN 2
     WHEN pd_mix_cd = "3P" THEN 3 ELSE NULL END AS RGUS_EOM
FROM UsefulFields 
WHERE load_dt=LAST_DAY(load_dt,month) AND (fi_outst_age<=90 OR fi_outst_age is null)
)
,CustomerStatus AS(
SELECT f.act_acct_cd AS Facct,l.act_acct_cd AS Lacct
,CASE WHEN f.act_acct_cd IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM
,CASE WHEN l.act_acct_cd IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM
FROM FirstDayRGU f FULL JOIN LastDayRGU l ON f.act_acct_cd=l.act_acct_cd
)
SELECT u.Month,u.act_acct_cd,s.ActiveBOM,s.ActiveEOM
FROM UsefulFields u INNER JOIN CustomerStatus s ON (u.act_acct_cd=Facct and u.act_acct_cd=Lacct)
--SELECT * FROM CustomerStatus 

--Flag de tecnología para cuando se arregle el flag del status
/*,TechnologyFlag AS(
SELECT *
,CASE WHEN LENGTH(CAST(act_acct_cd AS STRING))=8 THEN "HFC" 
          WHEN pd_bb_prod_nm LIKE "%GPON%" or pd_bb_prod_nm LIKE "%FTT%" then "FTTH" 
          ELSE "COPPER" END AS TechFlag
FROM tablaquetengaelstatusbien
)*/

