with
fmc_table as(
select * FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where month=date(dt)
)
,clean_interaction_time as (
select distinct *,DATE_TRUNC ('Month',cast(substr(cast(interaction_start_time as varchar),1,10) as date)) AS Month
from "db-stage-prod"."interactions_cwp"
    WHERE (cast(INTERACTION_START_TIME as varchar) != ' ') AND(INTERACTION_START_TIME IS NOT NULL)
)
,New_Insts as (
Select distinct f.month,count(distinct act_acct_cd) as Installs
from fmc_table f left join "lla_cco_int_stg"."cwp_fix_union_dna" u on f.month=DATE_TRUNC('month',date(u.dt)) and f.fixedaccount=u.act_acct_cd
where act_cust_typ_nm = 'Residencial' and date_trunc('month',date(u.dt)) = DATE_TRUNC('month',date(u.act_acct_inst_dt))
group by 1
)
,Repairs_flag as(
select distinct f.month,count(distinct fixedaccount) as repairs
FROM fmc_table f left join clean_interaction_time c on f.month=c.month and f.fixedaccount=c.account_id
WHERE interaction_purpose_descrip = 'TICKET' AND interaction_status ='CLOSED'
group by 1 
)
select distinct month,installs --date_trunc('month',instdate) as Inst_Month,count(distinct act_acct_cd)
from new_insts 
--group by 1,2
order by 1,2
