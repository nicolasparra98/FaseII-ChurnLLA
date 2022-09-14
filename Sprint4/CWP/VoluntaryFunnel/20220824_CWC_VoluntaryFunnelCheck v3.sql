with FMC_Table AS
( SELECT * FROM  "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where Month = date(dt)
)
,Original_Voluntary as(
SELECT distinct date(dt) as month,canal,retenidos,account_number
FROM "lla_cco_int_ext_prod"."cwp_ext_retention"
)
,Voluntary_Gap as(
select distinct o.*
from Original_Voluntary o LEFT join FMC_Table f on f.fixedaccount=cast(o.account_number as varchar) and f.month=o.month
where canal='RCOE' and fixedaccount is null 
)
,DNA AS(
SELECT distinct *
,case when overdue_bom >90 then 'Overdue' else null
--cast(Overdue_bom as varchar) 
end as bom_overdue
,case when overdue_eom >90 then 'Overdue' else null
--cast(overdue_eom as varchar) 
end as eom_overdue
,case when date_bom<>month_dna then 'Inactive' else null end as date_bom_adj
,case when date_eom<>lastday then 'Inactive' else null end as date_eom_adj
from(select distinct date_trunc('month',date(dt)) as month_dna, date(dt) as dt,act_acct_cd,fi_outst_age,pd_mix_cd,pd_bb_prod_cd,pd_bb_prod_nm
,first_value(date(dt)) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt asc) as date_bom
,first_value(fi_outst_age) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt asc) as overdue_bom
,first_value(date(dt)) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt desc) as date_eom
,first_value(fi_outst_age) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt desc) as overdue_eom
,date_trunc('month',date(dt)) + interval '1' MONTH - interval '1' day as lastday
FROM "db-analytics-prod"."fixed_cwp"  
WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial')
)
select distinct month,account_number,pd_bb_prod_nm,fi_outst_age
--month_dna,act_acct_cd,date_bom,overdue_bom,date_eom,overdue_eom
--month,pd_bb_prod_nm,count(distinct account_number)
--month,date_bom_adj,bom_overdue,date_eom_adj,eom_overdue,count(distinct account_number)
from dna d inner join voluntary_gap v on d.act_acct_cd=cast(account_number as varchar) and month_dna=month
--where date_eom=date('2022-02-28') and eom_overdue is null
--where fi_outst_age>200
--where bom_overdue='Overdue' or eom_overdue='Overdue' or date_bom_adj='Inactive' or date_eom_adj='Inactive'
where (date_bom_adj is null or date_eom_adj is null) and (bom_overdue='Overdue' or eom_overdue='Overdue') and pd_bb_prod_nm like '%Mora%'
--group by 1,2,3,4,5 order by 1,2,3,4,5
--order by 1,3,2

--group by 1,2 order by 1,2
