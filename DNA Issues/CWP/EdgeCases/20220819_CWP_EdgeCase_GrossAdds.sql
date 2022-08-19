WITH FMC_TABLE AS(
SELECT distinct month,fixedaccount,fixed_e_maxstart,e_fixedtenure,e_overdue,fixedmainmovement,fixedspinmovement,fixedchurnflag,fixedchurntype,fixedchurnsubtype
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"  where month=date(dt)  --LIMIT 10
AND fixed_rejoinermonth=0 AND fixedmainmovemenT='4.New Customer'
and month=date('2022-06-01')
--order by fixedaccount,month
)
,DNA AS(
SELECT distinct act_acct_cd
,first_value(date(dt)) over(partition by act_acct_cd order by date(dt)) as first_date
,first_value(date(act_cust_strt_dt)) over(partition by act_acct_cd order by date(dt)) as first_start_date
,first_value(date(act_acct_inst_dt)) over(partition by act_acct_cd order by date(dt)) as first_inst_date
--,date_trunc('month',date(dt)) as month, date(dt) as date,act_acct_cd,date(act_cust_strt_dt) as act_cust_strt_dt,date(act_acct_inst_dt) as act_acct_inst_dt,fi_outst_age
FROM "db-analytics-prod"."fixed_cwp"  WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial' 
--AND fi_outst_age>100
--and act_acct_cd='321052460000'
order by 1,2,3
)
select *
from fmc_table f inner join dna a on f.fixedaccount=a.act_acct_cd
--where date_trunc('month',date(a.date))<=f.month
where date_trunc('month',first_start_date)=date('2022-05-01')
order by 2,1
