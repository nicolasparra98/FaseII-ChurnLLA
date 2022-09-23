--CREATE TABLE "ba-lla-cwc-dev"."polaris_sample_dna_accounts" as 

with base_table as (select *,date(date_parse(date,'%Y-%m-%d %H:%i:%s')) as date_register,date_trunc('month',date(date_parse(date,'%Y-%m-%d %H:%i:%s'))) as month_pol from  "lla_polaris"."ivr_calls" 
where market = 'JM' and (lower(lob) like '%fixed%' or lower(lob) = 'all') --and date(date_parse(date,'%Y-%m-%d %H:%i:%s')) between date('2022-08-01') and date('2022-08-31') --and lower(lob) = 'fixed'--and channel = 'IVR' and lower(lob) = 'fixed'
--and agentskilltargetid != '' and call_service_type = 'Transfer to Agent'--and lower(wrapupdata) like '%bb%tech%'
--group by extract (week from date_register) 
--ORDER BY week_reg
)
,dna_accounts as (
select *, count() over (partition by last_act_contact_phone_1) as total_num_p1,
count() over (partition by last_act_contact_phone_2) as total_num_p2, 
count() over (partition by last_act_contact_phone_3) as total_num_p3
from (select distinct (act_acct_cd) as act_acct_cd, date_trunc('month',date(dt)) as month_dna
,first_value(act_contact_phone_1) over(partition by act_acct_cd order by dt desc) as last_act_contact_phone_1
,first_value(act_contact_phone_2) over(partition by act_acct_cd order by dt desc) as last_act_contact_phone_2
,first_value(act_contact_phone_3) over(partition by act_acct_cd order by dt desc) as last_act_contact_phone_3
from "db-analytics-prod"."tbl_fixed_cwc"
where org_cntry = 'Jamaica' --and  date(dt) BETWEEN date('2022-08-01') and date('2022-08-31')
)
)
,dna_accounts_filter as (
select *, case when total_num_p1 = 1 then last_act_contact_phone_1 end as final_act_contact_phone_1,
case when total_num_p2 = 1 then last_act_contact_phone_2 end as final_act_contact_phone_2,
case when total_num_p3 = 1 then last_act_contact_phone_3 end as final_act_contact_phone_3
from dna_accounts
)
,join_dna as (
select a.*, b.act_acct_cd as act_dna_on_var1, c.act_acct_cd as act_on_acct_nbr
from base_table a
left join dna_accounts b
on cast(a.variable1 as varchar)= b.act_acct_cd and month_pol=b.month_dna
left join dna_accounts c
on cast(a.account_nbr as varchar)= c.act_acct_cd and month_pol=c.month_dna
)
,no_dna_account_final as (
select *
from (select *, case when act_dna_on_var1 is null then act_on_acct_nbr else act_dna_on_var1 end as act_dna_final
from join_dna)
where act_dna_final is not null
)
select distinct --date(date_parse(date,'%Y-%m-%d %H:%i:%s')),
month_pol,count(distinct act_dna_final)
from no_dna_account_final 
group by 1 order by 1

/*
select * from "ba-lla-cwc-dev"."polaris_sample_ani_accounts" union all 

select *, case when act_dna_final is not null then null end as act_acct_cd_1, case when act_dna_final is not null then null end as act_acct_cd_2,
case when act_dna_final is not null then null end as act_acct_cd_3, case when act_dna_final is not null then null end as act_acct_final
from no_dna_account_final
*/
