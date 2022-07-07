with
fmc_table as(
select * FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where month=date(dt)
)
,clean_interaction_time as (
select distinct *,DATE_TRUNC ('Month',cast(substr(cast(interaction_start_time as varchar),1,10) as date)) AS Month
from "db-stage-prod"."interactions_cwp"
    WHERE (cast(INTERACTION_START_TIME as varchar) != ' ') AND(INTERACTION_START_TIME IS NOT NULL)
)
,Repairs_flag as(
select distinct f.month,b_numrgus,fixedaccount
,case when interaction_purpose_descrip = 'TRUCKROLL' AND interaction_status ='CLOSED' then fixedaccount end as repairs_users
,case when interaction_purpose_descrip = 'TRUCKROLL' AND interaction_status ='CLOSED' then interaction_id end as repairs
,case when interaction_purpose_descrip IN('TICKET','TRUCKROLL') AND interaction_status ='CLOSED' then interaction_id end as Tech_Calls
,case when interaction_purpose_descrip = 'CLAIM' AND interaction_status ='CLOSED' then interaction_id end as Care_Calls
FROM fmc_table f left join clean_interaction_time c on f.month=c.month and f.fixedaccount=c.account_id
--where f_activebom=1
--group by 1,2,interaction_purpose_descrip,interaction_status
)
select distinct month,round(cast(sum(repairs) as double)/cast(sum(rgus) as double)*100,2) as Repairs_per_100_rgus,round(cast(sum(tech_calls) as double)/cast(sum(rgus) as double)*100,2) as TechCalls_per_100_rgus,round(cast(sum(care_calls) as double)/cast(sum(rgus) as double)*100,2) as Care_Calls_per_100_rgus
--,sum(users) as total_users,sum(rgus) as total_rgus,sum(repairs_users) as repairs_users,sum(repairs) as repairs
from(
select distinct month,b_numrgus,count(distinct fixedaccount) as users,b_numrgus*count(distinct fixedaccount) as rgus,count(distinct repairs_users) as repairs_users,count(distinct repairs) as repairs,count(distinct tech_calls) as tech_Calls,count(distinct care_calls) as care_calls
from repairs_flag 
group by 1,2
)
group by 1 order by 1
