WITH
New_Customers as (
SELECT DISTINCT
DATE_TRUNC('MONTH',CAST(load_dt AS DATE)) AS Month_load,CAST(load_dt AS DATE) AS load_dt,act_acct_cd,DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS Start_Month,CAST(act_cust_strt_dt AS DATE) AS StartDate,fi_outst_age,pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
,CASE WHEN fi_outst_age=46 AND DATE_DIFF('month',DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE)),DATE_TRUNC('month',CAST(load_dt AS DATE)))<=2 then 'soft_dx'  
      WHEN fi_outst_age=90 AND DATE_DIFF('month',DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE)),DATE_TRUNC('month',CAST(load_dt AS DATE)))<=3 then 'hard_dx'
      ELSE NULL END as dx_flag
from "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial' AND act_cust_typ_nm = 'Residencial'
AND act_acct_typ_grp ='MAS MOVIL'
AND DATE_DIFF('month',DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE)),DATE_TRUNC('month',CAST(load_dt AS DATE)))<=3
GROUP BY 1,2,3,4,5,6,7,8,9
)
,Tech_Flag AS(
SELECT DISTINCT Month_load,load_dt,start_month,startdate,act_acct_cd,dx_flag
,CASE WHEN (PD_BB_ACCS_MEDIA='FTTH' OR PD_TV_ACCS_MEDIA ='FTTH' OR PD_VO_ACCS_MEDIA='FTTH') THEN 'FTTH'
      WHEN (PD_BB_ACCS_MEDIA='HFC' OR PD_TV_ACCS_MEDIA ='HFC' OR PD_VO_ACCS_MEDIA='HFC') THEN 'HFC'
      WHEN (PD_BB_ACCS_MEDIA='VDSL' OR PD_TV_ACCS_MEDIA ='VDSL' OR PD_VO_ACCS_MEDIA='VDSL' OR 
            PD_BB_ACCS_MEDIA='COPPER' OR PD_TV_ACCS_MEDIA ='COPPER' OR PD_VO_ACCS_MEDIA='COPPER') THEN 'COPPER'
      ELSE 'Other' END AS TechFlag
FROM New_Customers
)
SELECT DISTINCT Month_load,Start_Month,dx_flag,COUNT(DISTINCT act_acct_cd) AS Records
FROM Tech_Flag
--WHERE Month_load=DATE('2022-04-01')
--AND DX_FLAG='soft_dx'
GROUP BY 1,2,3
ORDER BY 1,2,3
