with base_table as (select *,date(date_parse(date,'%Y-%m-%d %H:%i:%s')) as date_register,date_trunc('month',date(date_parse(date,'%Y-%m-%d %H:%i:%s'))) as month_pol from  "lla_polaris"."ivr_calls"
where market = 'JM' and (lower(lob) like '%fixed%' or lower(lob) = 'all') 
--and date(date_parse(date,'%Y-%m-%d %H:%i:%s')) between date('2022-08-01') and date('2022-08-31')
)
,dna_accounts as (
select *, count() over (partition by last_act_contact_phone_1,month_dna) as total_num_p1,
count() over (partition by last_act_contact_phone_2,month_dna) as total_num_p2, 
count() over (partition by last_act_contact_phone_3,month_dna) as total_num_p3
from (select distinct (act_acct_cd) as act_acct_cd, date_trunc('month',date(dt)) as month_dna
,first_value(act_contact_phone_1) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt desc) as last_act_contact_phone_1
,first_value(act_contact_phone_2) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt desc) as last_act_contact_phone_2
,first_value(act_contact_phone_3) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt desc) as last_act_contact_phone_3
from "db-analytics-prod"."tbl_fixed_cwc"
where org_cntry = 'Jamaica' 
--and  date(dt) BETWEEN date('2022-08-01') and date('2022-08-31')
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
where act_dna_final is null
)
,join_numbers as (
select a.*
, b.act_acct_cd as act_acct_cd_1,b.month_dna
,c.act_acct_cd as act_acct_cd_2,c.month_dna
,d.act_acct_cd as act_acct_cd_3,d.month_dna
from no_dna_account_final a
left join dna_accounts_filter b
on a.ani = b.final_act_contact_phone_1 and month_pol=b.month_dna
left join dna_accounts_filter c
on a.ani = c.final_act_contact_phone_2 and month_pol=c.month_dna
left join dna_accounts_filter d
on a.ani = d.final_act_contact_phone_3 and month_pol=d.month_dna
)
,final_table as (
select *, case when act_acct_cd_1 is not null then act_acct_cd_1 else (case when act_acct_cd_2 is not null then act_acct_cd_2 else act_acct_cd_3 end) 
end as act_acct_final
from join_numbers
)
select distinct --date(date_parse(date,'%Y-%m-%d %H:%i:%s')),
month_pol,count(*), count(distinct ani),count(distinct act_acct_final)
from final_table
where act_acct_final is not null-- and call_service_type = 'Transfer to Agent'
group by 1 order by 1
