WITH 

FirstDayRGU AS(
SELECT act_acct_cd, DATE_TRUNC(load_dt,Month) AS Month,load_dt
 ,pd_vo_prod_cd, pd_bb_prod_cd, pd_tv_prod_cd
 ,pd_mix_cd
 ,CASE WHEN pd_vo_prod_cd IS NOT NULL AND pd_vo_prod_cd<>"" THEN 1 ELSE 0 END AS VOF
 ,CASE WHEN pd_bb_prod_cd IS NOT NULL AND pd_bb_prod_cd<>"" THEN 1 ELSE 0 END AS BBF
 ,CASE WHEN pd_tv_prod_cd IS NOT NULL AND pd_tv_prod_cd<>"" THEN 1 ELSE 0 END AS TVF
     --ELSE NULL END AS RGUSFirst
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt=DATE_TRUNC(load_dt,Month)
 AND (fi_outst_age <90 OR fi_outst_age is null)
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)
,LastDayRGU AS(
SELECT act_acct_cd, DATE_TRUNC(load_dt,Month) AS Month
 ,pd_vo_prod_cd, pd_bb_prod_cd, pd_tv_prod_cd
 ,pd_mix_cd
 ,CASE WHEN pd_vo_prod_cd IS NOT NULL AND pd_vo_prod_cd<>"" THEN 1 ELSE 0 END AS VOL
 ,CASE WHEN pd_bb_prod_cd IS NOT NULL AND pd_bb_prod_cd<>"" THEN 1 ELSE 0 END AS BBL
 ,CASE WHEN pd_tv_prod_cd IS NOT NULL AND pd_tv_prod_cd<>"" THEN 1 ELSE 0 END AS TVL
     --ELSE NULL END AS RGUSLast
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt=LAST_DAY(load_dt,month)
 AND (fi_outst_age <90 OR fi_outst_age is null)
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)
,Classification AS(
SELECT l.act_acct_cd,l.Month,f.VOF,f.BBF,f.TVF,l.VOL,l.BBL,l.TVL
,f.pd_vo_prod_cd, f.pd_bb_prod_cd, f.pd_tv_prod_cd,l.pd_vo_prod_cd, l.pd_bb_prod_cd, l.pd_tv_prod_cd
,f.pd_mix_cd,l.pd_mix_cd
, CASE WHEN VOL-VOF>0 THEN "Upsell"
     WHEN VOL-VOF<0 THEN "Downsell"
     WHEN VOL-VOF=0 THEN "Same" 
     ELSE "Null" END AS SellFlagVO
, CASE WHEN BBL-BBF>0 THEN "Upsell"
     WHEN BBL-BBF<0 THEN "Downsell"
     WHEN BBL-BBF=0 THEN "Same" 
     ELSE "Null" END AS SellFlagBB
, CASE WHEN TVL-TVF>0 THEN "Upsell"
     WHEN TVL-TVF<0 THEN "Downsell"
     WHEN TVL-TVF=0 THEN "Same" 
     ELSE "Null" END AS SellFlagTV
FROM FirstDayRGU f INNER JOIN LastDayRGU l ON f.act_acct_cd=l.act_acct_cd
--WHERE f.VO IS NULL
GROUP BY l.act_acct_cd,l.Month,f.VOF,f.BBF,f.TVF,l.VOL,l.BBL,l.TVL
,f.pd_vo_prod_cd, f.pd_bb_prod_cd, f.pd_tv_prod_cd,l.pd_vo_prod_cd, l.pd_bb_prod_cd, l.pd_tv_prod_cd
,f.pd_mix_cd,l.pd_mix_cd
)

SELECT --SellFlagVO
--,
--SellFlagBB
--,
SellFlagTV
/*
, CASE WHEN SellFlagVO="Upsell" THEN COUNT(DISTINCT act_acct_cd) 
       WHEN SellFlagVO="Downsell" THEN COUNT(DISTINCT act_acct_cd)
       --WHEN SellFlagVO="Same" THEN COUNT(DISTINCT act_acct_cd) 
       END AS VO
*/
/*
, CASE WHEN SellFlagBB="Upsell" THEN COUNT(DISTINCT act_acct_cd) 
       WHEN SellFlagBB="Downsell" THEN COUNT(DISTINCT act_acct_cd)
       WHEN SellFlagBB="Same" THEN COUNT(DISTINCT act_acct_cd) END AS BB
*/
--/*
, CASE WHEN SellFlagTV="Upsell" THEN COUNT(DISTINCT act_acct_cd) 
       WHEN SellFlagTV="Downsell" THEN COUNT(DISTINCT act_acct_cd)
       WHEN SellFlagTV="Same" THEN COUNT(DISTINCT act_acct_cd) END AS TV
--*/
FROM Classification
GROUP BY --SellFlagVO
--,
--SellFlagBB
--,
SellFlagTV
