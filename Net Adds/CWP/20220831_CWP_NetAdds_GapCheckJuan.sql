with
dna as(
select distinct date_trunc('month',date(dt)) as month,date(dt) as dt, act_acct_cd,fi_outst_age,pd_mix_cd,pd_bb_prod_cd,pd_tv_prod_cd,pd_vo_prod_cd
,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm
from "db-analytics-prod"."fixed_cwp"
where 
--Ejemplos pd mix
/*
(act_acct_cd='212038450000' and date(dt) between date('2022-04-30') and date('2022-06-01'))
or (act_acct_cd='157063500000' and date(dt) between date('2022-06-30') and date('2022-08-01'))
or (act_acct_cd='217002270000' and date(dt) between date('2022-04-30') and date('2022-06-01'))
*/
--Ejemplos Voluntary
--/*
(act_acct_cd='311051800000' and date(dt) between date('2022-01-31') and date('2022-05-01'))
or (act_acct_cd='285007900000' and date(dt) between date('2021-12-31') and date('2022-02-01'))
or (act_acct_cd='801348380000' and date(dt) between date('2022-03-31') and date('2022-05-01'))
or (act_acct_cd='319009860000' and date(dt) between date('2022-03-31') and date('2022-05-01'))
--*/
)
,SO_flag AS(
Select distinct 
date_trunc('Month', date(completed_date)) as month,date(completed_date) as EndDate,date(order_start_date) as StartDate
,order_type,ORDER_STATUS,cease_reason_code, cease_reason_desc,cease_reason_group
,CASE WHEN cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91') THEN 'Voluntario'
 WHEN cease_reason_code IN('2','74') THEN 'Involuntario'
 WHEN (cease_reason_code = '9' AND cease_reason_desc='CAMBIO DE TECNOLOGIA') OR (cease_reason_code IN('32','44','55','72')) THEN 'Migracion'
 WHEN cease_reason_code = '9' AND cease_reason_desc<>'CAMBIO DE TECNOLOGIA' THEN 'Voluntario'
ELSE NULL END AS DxType
,account_id
,lob_vo_count,lob_bb_count,lob_tv_count
from "db-stage-dev"."so_hdr_cwp" 
where order_type = 'DEACTIVATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
)
select *
from dna l left JOIN so_flag v ON CAST(v.account_id AS VARCHAR)=l.act_acct_Cd AND --v.month = l.Month
v.EndDate=l.dt
order by 3,2
/*
with
voluntary as(
select distinct month,fixedaccount
from "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where Month = date(dt)
and fixedchurntype='1. Fixed Voluntary Churner'
and f_activeeom=1
)
select distinct a.month--,NextMonth
,NextActiveEOM
,count(distinct a.fixedaccount),sum(b_numrgus)
--,fixedaccount
from(select distinct month,f_activeeom--,count(distinct fixedaccount),sum(b_numrgus)
,lag(f_activeeom) over(partition by fixedaccount order by month desc) as NextActiveEOM
,lag(month) over(partition by fixedaccount order by month desc) as NextMonth
,lag(f_activeeom) over(partition by fixedaccount order by month desc) as NextActiveEOM2
,lag(month) over(partition by fixedaccount order by month desc) as NextMonth2
,fixedaccount
,b_numrgus
,fixedchurntype
FROM  "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where Month = date(dt)
--and fixedchurntype='1. Fixed Voluntary Churner'
--and f_activeeom=1
--and fixedaccount='311051800000'
--'801348380000' rejoiner
--group by 1,2 order by 1,2
)
a inner join voluntary v on a.fixedaccount=v.fixedaccount and a.month=v.month
--where --fixedaccount in(select distinct fixedaccount from voluntary)and
--fixedchurntype='1. Fixed Voluntary Churner' --and nextActiveEOM=1
group by 1,2--,3
order by 1,2--,3
*/
