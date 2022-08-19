WITH FMC_TABLE AS(
SELECT *
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where month=date(dt)  --LIMIT 10
AND fixed_rejoinermonth=0 AND fixedmainmovemenT='5.Come Back to Life'
AND FIXEDACCOUNT IN('112027280000','259017560000','181021460000','309022080000','126020910000')
--order by 2,1
)
,DNA AS(
SELECT distinct date_trunc('month',date(dt)) as month, date(dt) as date,act_acct_cd,fi_outst_age
FROM "db-analytics-prod"."fixed_cwp"  WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial' 
--AND fi_outst_age>100
and act_acct_cd='309022080000'
order by 1,2
)
select f.*,date_trunc('month',date(a.dt)) as month_fi,fi_outst_age
from fmc_table f inner join dna a on f.fixedaccount=a.act_acct_cd
where date_trunc('month',date(a.dt))=date_add('month',-1,f.month)
