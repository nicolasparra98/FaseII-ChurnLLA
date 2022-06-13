with 
mayo_migracion as(
select distinct * --fixedmonth,fixedchurnflag,fixedchurntype,FixedChurnSubtype,count(distinct fixedaccount)
from "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_RJ_v3_mayo2"
where fixedmonth=date('2022-05-01') and FixedChurnSubtype='Incomplete CST' 
--group by 1,2,3,4 
--order by 1,2,3,4
)
,abril_migracion as(
select distinct 
date(LOAD_DT) as load_dt
,ACT_ACCT_CD AS FixedAccount,ACT_CONTACT_PHONE_3 AS CONTACTO
,FI_OUTST_AGE,MAX(CAST(CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart, round(FI_TOT_MRC_AMT,0) AS Fixed_MRC
,Case When pd_bb_accs_media = 'FTTH' Then 'FTTH'
        When pd_bb_accs_media = 'HFC' Then 'HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'
    ELSE 'COPPER' end as TechFlag
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numBB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numVO
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS BB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS TV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS VO
,PD_BB_PROD_CD, pd_tv_prod_cd, PD_VO_PROD_CD, pd_mix_nm,pd_mix_cd
--fixedmonth,fixedchurnflag,fixedchurntype,FixedChurnSubtype,count(distinct fixedaccount)
FROM "db-analytics-prod"."fixed_cwp"
where act_cust_typ_nm = 'Residencial'  and DATE_TRUNC('MONTH',DATE(LOAD_dt))=date('2022-06-01') 
and PD_MIX_CD='0P'
group by 1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18
)
select distinct --f.* --f.fixedchurnflag,f.fixedchurntype,f.fixedchurnsubtype,
count(distinct f.fixedaccount)
from mayo_migracion m inner join abril_migracion f on m.fixedaccount=f.fixedaccount
--group by 1,2,3
--order by 1,2,3
