with
new_cust as(
select distinct DATE_TRUNC('MONTH',CAST(load_dt AS DATE)) AS Month_load,CAST(load_dt AS DATE) AS load_dt,act_acct_cd,DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS Start_Month,CAST(act_cust_strt_dt AS DATE) AS StartDate,fi_outst_age,fi_bill_due_dt_m0
,CASE WHEN fi_outst_age=46 AND DATE_DIFF('month',DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE)),DATE_TRUNC('month',CAST(load_dt AS DATE)))<=2 then 'soft_dx'  
      WHEN fi_outst_age=90 AND DATE_DIFF('month',DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE)),DATE_TRUNC('month',CAST(load_dt AS DATE)))<=3 then 'hard_dx'
      ELSE NULL END as dx_flag
from "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial' AND act_cust_typ_nm = 'Residencial'
AND act_acct_typ_grp ='MAS MOVIL'
AND DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE))=DATE_TRUNC('month',CAST(load_dt AS DATE))
)
select distinct start_month,count(distinct act_acct_cd)
from new_cust
group by 1 order by 1
