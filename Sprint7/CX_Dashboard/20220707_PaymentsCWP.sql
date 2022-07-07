with
payments as(
select 'CWP' as opco,'Panama' as country_name,count(distinct(payment_doc_id)) as pymt_cnt,'Non_Digital' as pymnt_typ,date_trunc('month',date(dt)) as month
FROM "db-stage-prod"."payments_cwp"
where account_type = 'B2C' and digital_nondigital in('In Person', 'In person')
group by 1,2,5
union all
select 'CWP' as opco,'Panama' as country_name,count(distinct(payment_doc_id)) as pymt_cnt,'Digital' as pymnt_typ,date_trunc('month',date(dt)) as month
FROM "db-stage-prod"."payments_cwp"
where account_type = 'B2C'	and digital_nondigital = 'Digital'
group by 1,2,5
)
select distinct month, pymnt_typ,pymt_cnt
from payments
order by 1

select distinct month,opco,market,sum(digital) as digital,sum (pymt_cnt) as total,round(cast(sum(digital) as double)/cast(sum (pymt_cnt) as double),2) as kpi
from(
select 'CWP' as opco,'Panama' as market,count(distinct(payment_doc_id)) as pymt_cnt,'Non_Digital' as pymnt_typ,date_trunc('month',date(dt)) as month
,case when digital_nondigital = 'Digital' then count(distinct(payment_doc_id)) end as digital
FROM "db-stage-prod"."payments_cwp"
where account_type = 'B2C'
group by 1,2,5,digital_nondigital)
group by 1,2,3
