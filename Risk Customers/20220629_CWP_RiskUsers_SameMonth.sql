with 
FMC_Table AS
( SELECT * FROM  "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where Month = date(dt) and fixedaccount is not null
)
,interactions as (
select date_trunc('month',date(INTERACTION_START_TIME)) as int_month,ACCOUNT_ID, interaction_id,date(INTERACTION_START_TIME) as interaction_start_date
from "db-stage-prod"."interactions_cwp"
where interaction_id is not null 
order by interaction_start_date
)
,interactions_panel as (
select int_month,ACCOUNT_ID as interactions_account_id, count(distinct interaction_start_date) as num_distinct_interact_days_all
from interactions
group by int_month,ACCOUNT_ID
)
,SIR_model as (
Select date_trunc('month',date(year_month)) as sir_month,*
FROM "db-stage-prod"."scores_001_cwp"
)
,join_SIR_Interactions as (
SELECT
a.*,
b.num_distinct_interact_days_all
FROM SIR_model as a
LEFT JOIN interactions_panel as b ON CAST(a.act_acct_cd as VARCHAR) = CAST(b.interactions_account_id as VARCHAR) and int_month=sir_month
)
,Risk_Customers as(
select * from(SELECT *,CASE WHEN percentile_rank_score >=90 OR num_distinct_interact_days_all >= 4 THEN 'Alto_riesgo' ELSE 'Riesgo_medio' END AS Riskcustomer
FROM join_SIR_Interactions )
where Riskcustomer='Alto_riesgo'
)
,Risk_Flag as(
select f.*
,case when fixedchurntype ='1. Fixed Voluntary Churner' then fixedaccount else null end as Vol_Churner
,case when fixedchurntype ='1. Fixed Voluntary Churner' then b_numrgus else null end as Vol_RGUs
,case when (fixedchurntype <> '1. Fixed Voluntary Churner' or fixedchurntype is null) then fixedaccount else null end as Non_Vol_Churner
,case when r.act_acct_cd is not null then 1 else 0 end as Risk_Customer
from fmc_table f left join risk_customers r on f.month=r.sir_month and f.fixedaccount=cast(r.act_acct_cd as varchar)
)

select distinct month,Risk_Customer,count(distinct fixedaccount) as Total_Accounts,count(distinct vol_churner) as Churners,sum(b_numrgus) as Total_RGUs,sum(Vol_RGUs) as Churn_RGUs
from risk_flag
where f_activebom=1
group by 1,2 order by 1,2
