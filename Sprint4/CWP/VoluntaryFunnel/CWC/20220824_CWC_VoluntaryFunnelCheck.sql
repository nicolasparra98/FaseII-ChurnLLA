WITH 
--calling the bases for rgus
--Dissconnection
diss as (
SELECT account_number as diss_id,lower(disconnected_services) as diss_services,date(date_trunc('Month', date_parse(service_end_dt, '%m/%d/%Y'))) as month,department as department
,CASE WHEN lower(disconnected_services) = 'Click' THEN 'BO'
	  when lower(disconnected_services) = 'watch' THEN 'TV'
	  when lower(disconnected_services) = 'Talk' THEN 'VO'
	  when lower(disconnected_services) = 'NA' THEN 'NA' --HAY NA
	  when lower(disconnected_services) = 'na,click' THEN 'BO' --HAY NA
	  when lower(disconnected_services) = 'Watch,click' THEN 'BO+TV'
	  when lower(disconnected_services) = 'talk,click' THEN 'BO+VO'
	  when lower(disconnected_services) = 'Watch,talk' THEN 'VO+TV'
	  when lower(disconnected_services) = 'Watch,talk,click ' THEN 'BO+VO+TV'
	  when lower(disconnected_services) = 'talk,mobile,click' THEN 'BO+VO' --HAY MOBILE
		end as dis_mixname
FROM "lla_cco_int_ext_prod"."cwc_ext_disconnections"
where disconnected_services <> 'MOBILE' --and Department = 'RCOE'
)
,mvm as (
select id as mvm_id,TypeRet,date(date_parse(mth, '%m/%d/%Y')) as month
from(SELECT cc as ID,"completion mth" as mth,case when play_cl >= play_op then 'total_retention'
	  when play_cl < play_op then 'partial_retention'
end as TypeRet
FROM "lla_cco_int_ext_prod"."cwc_ext_retention")
)
,full_ret_base as(
select 
case when mvm_id is not null and diss_id is not null then m.month
     when mvm_id is not null and diss_id is null then m.month
     when mvm_id is null and diss_id is not null then d.month
end as month
,diss_id,department,diss_services,dis_mixname,mvm_id,TypeRet
from diss d full outer join mvm m on d.month=m.month and d.diss_id=m.mvm_id
)
,ret_base_flags as(
select distinct *
,case when mvm_id is not null and diss_id is not null then mvm_id
      when mvm_id is not null and diss_id is null then mvm_id
      when mvm_id is null and diss_id is not null then diss_id
end as intents
from full_ret_base
)
,FMC_Table AS
( SELECT * FROM  "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" where Month = date(dt)
)
,voluntary_gap as(
select distinct r.month,intents
--,count(distinct intents) as intents,count(distinct mvm_id) as retained, count(distinct diss_id) as Not_Retained
from ret_base_flags r left join fmc_table f on r.month=f.month and cast(r.intents as varchar)=f.fixed_account
where f.fixed_account is null
--group by 1 order by 1
)
,DNA AS(
SELECT distinct *
,case when cast(overdue_bom as double) >90 then 'Overdue' else cast(Overdue_bom as varchar) end as bom_overdue
,case when cast(overdue_eom as double) >90 then 'Overdue' else cast(overdue_eom as varchar) end as eom_overdue
from(select distinct date_trunc('month',date(dt)) as month_dna, date(dt) as dt,act_acct_cd,fi_outst_age,pd_mix_cd
,first_value(date(dt)) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt asc) as date_bom
,first_value(fi_outst_age) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt asc) as overdue_bom
,first_value(date(dt)) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt desc) as date_eom
,first_value(fi_outst_age) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt desc) as overdue_eom
FROM "db-analytics-prod"."tbl_fixed_cwc" 
  WHERE org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard') 
  AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
  )
)
,remaining_gap as(
select distinct v.* 
--month,date_bom,bom_overdue,date_eom,eom_overdue,count(distinct intents)
from dna d right join voluntary_gap v on d.act_acct_cd=cast(intents as varchar) and month_dna=month
--where date_eom=date('2022-02-28') and eom_overdue is null
--group by 1,2,3,4,5 order by 1,2,3,4,5
where d.act_acct_cd is null
--order by 1,3,2
)
,DNA_2 AS(
SELECT distinct *
--,case when cast(overdue_bom as double) >90 then 'Overdue' else cast(Overdue_bom as varchar) end as bom_overdue
--,case when cast(overdue_eom as double) >90 then 'Overdue' else cast(overdue_eom as varchar) end as eom_overdue
from(select distinct date_trunc('month',date(dt)) as month_dna, date(dt) as dt,act_acct_cd,fi_outst_age,pd_mix_cd,fi_tot_mrc_amt,org_cntry,ACT_CUST_TYP_NM,ACT_ACCT_STAT
,first_value(date(dt)) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt asc) as date_bom
,first_value(fi_outst_age) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt asc) as overdue_bom
,first_value(date(dt)) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt desc) as date_eom
,first_value(fi_outst_age) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by dt desc) as overdue_eom
FROM "db-analytics-prod"."tbl_fixed_cwc" 
  --WHERE org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard') 
  --AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
  )
)
select distinct --* --v.month,count(distinct intents)
month,ACT_CUST_TYP_NM,count(distinct intents)
--month,date_bom,bom_overdue,date_eom,eom_overdue,count(distinct intents)
from dna_2 d right join remaining_gap v on d.act_acct_cd=cast(intents as varchar) and month_dna=month
--where date_eom=date('2022-02-28') and eom_overdue is null
--where month=date('2022-03-01')
--group by 1,2,3,4,5 order by 1,2,3,4,5
--where d.act_acct_cd is null
where ACT_CUST_TYP_NM not IN ('Browse & Talk HFONE', 'Residence','Standard') or ACT_CUST_TYP_NM is null
group by 1,2 order by 1,2
--group by 1 order by 1
--order by 1,3,2
