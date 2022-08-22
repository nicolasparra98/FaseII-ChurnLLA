WITH FMC_TABLE1 AS(
SELECT distinct month,fixedaccount,b_overdue,fixedmainmovement,fixedspinmovement,fixedchurnsubtype
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"  where month=date(dt)  --LIMIT 10
AND fixedchurntype is not null
order by fixedaccount,month
)
,FMC_TABLE2 AS(
SELECT distinct month,fixedaccount,b_overdue,fixedmainmovement,fixedspinmovement,fixedchurnsubtype
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"  where month=date(dt)  --LIMIT 10
AND fixedchurntype is not null
order by fixedaccount,month
)
,repeated_churners as(
select distinct *,a.fixedaccount as acct
from fmc_table1 a inner join fmc_table2 b on a.fixedaccount=b.fixedaccount
where a.month=date_add('month',-1,b.month)
)
/*
select distinct month,fixedchurnsubtype,nextchurn,count(distinct fixedaccount)
from(
select f.*
,lag(fixedchurnsubtype) over(partition by fixedaccount order by month desc) as NextChurn
from fmc_table1 f 
where fixedaccount in(select acct from repeated_churners)
order by fixedaccount,month
--group by 1 order by 1
)
--where nextchurn is not null
--group by 1,2,3 order by 1,2,3
*/
/*,DNA AS(
SELECT distinct date_trunc('month',date(dt)) as month, date(dt) as date,act_acct_cd,fi_outst_age
FROM "db-analytics-prod"."fixed_cwp"  WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial' 
--AND fi_outst_age>100
and act_acct_cd='171080260000'
order by 1,2
)*/
,SO_flag AS(
Select distinct 
date_trunc('Month', date(completed_date)) as month,date(completed_date) as EndDate,date(order_start_date) as StartDate
,cease_reason_code, cease_reason_desc,cease_reason_group
,CASE 
 WHEN cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91') THEN 'Voluntario'
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
from so_flag
where account_id=171071220000

select *
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"  where month=date(dt) 
and fixedaccount='101020670000'
order by 1
