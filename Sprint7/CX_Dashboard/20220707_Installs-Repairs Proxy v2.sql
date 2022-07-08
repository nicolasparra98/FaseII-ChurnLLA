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
select distinct date_trunc('Month', date(completed_date)) as month,count(distinct account_id) as install_users,count(distinct order_id) as install
from "db-stage-dev"."so_hdr_cwp" 
where order_type='INSTALLATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
group by 1
)
select month,install_users,install
from new_insts
order by 1

,Repairs_flag as(
select distinct f.month,count(distinct fixedaccount) as repairs_users,count(distinct interaction_id) as repairs
FROM fmc_table f left join clean_interaction_time c on f.month=c.month and f.fixedaccount=c.account_id
WHERE interaction_purpose_descrip = 'TRUCKROLL' AND interaction_status ='CLOSED'
group by 1 
)
select distinct month,repairs_users,repairs --date_trunc('month',instdate) as Inst_Month,count(distinct act_acct_cd)
from repairs_flag 
--group by 1,2
order by 1,2,3
