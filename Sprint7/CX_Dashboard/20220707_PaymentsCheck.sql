with
fmc_table as(
select *
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where month=date(dt)
)

select date_trunc('month',date(p.dt)) as month,'CWP' as opco,'Panama' as market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,count(distinct(payment_doc_id)) as pymt_cnt,account_id,customer_full_name
,case when digital_nondigital = 'Digital' then count(distinct(payment_doc_id)) end as digital
FROM "db-stage-prod"."payments_cwp" p --inner join fmc_table f on p.account_id=f.fixedaccount and f.month=date_trunc('month',date(p.dt))
where account_type = 'B2C' and date_trunc('month',date(p.dt))=date('2022-04-01') and digital_nondigital = 'Digital'
group by 1,2,3,4,5,6,digital_nondigital,account_id,customer_full_name


select distinct date(dt) as dt,ACT_ACCT_CD AS FixedAccount,act_acct_name,ACT_CONTACT_PHONE_3 AS CONTACTO,act_cust_typ_nm,FI_OUTST_AGE,CAST(CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE) AS Start_dt, round(FI_TOT_MRC_AMT,0) AS Fixed_MRC
,PD_BB_PROD_CD, pd_tv_prod_cd, PD_VO_PROD_CD, pd_mix_nm,pd_mix_cd
from "lla_cco_int_stg"."cwp_fix_union_dna"
where date_trunc('month',date(dt))=date('2022-04-01') and act_acct_cd='293014100000'
--and lower(act_acct_name) like '%dafne%' --rodriguez%'
--where act_acct_cd like('%1166111%','%516765%','%1853581%') and date_trunc('month',date(dt))=date('2022-05-01')
--where act_acct_cd in ('163640600000','133243300000','188630500000') and date_trunc('month',date(dt))=date('2022-04-01')
order by 2,1

select distinct account_id,customer_id
FROM "db-stage-prod"."payments_cwp"
where account_id in ('1166111','516765','1853581','1636406','1332433','1886305')


select *
FROM "db-analytics-dev"."tbl_postpaid_cwp"
where accountno in('1636406','1793373')

with
fmc_table as(
select *
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where month=date(dt)
)
select *
from fmc_table
where mobile_account in('1412220','930384','1985592','1793373','1544777','2012624')
