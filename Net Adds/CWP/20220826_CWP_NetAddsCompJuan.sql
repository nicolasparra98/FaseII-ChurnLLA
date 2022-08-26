with parameters as (
select
--##############################################################
--### Change Date in this line to define paying period #########
date('2022-01-01') as start_date,
date('2022-08-01') as end_date,
90 as max_overdue_active_base

--##############################################################

),

list_dates as (
select distinct (date_trunc('month',date(dt))- interval '1' day) as months
from (
    select distinct dt
    from "db-analytics-prod"."fixed_cwp"
    where date(dt) between (select start_date from parameters) and (select end_date from parameters) 
    )
),

dna as (
select act_acct_cd, date(dt) as dt,
extract(week from date(dt)) as week_num,
max(cast(fi_outst_age as int)) as fi_outst_age,
date_diff('day',max(date(fi_bill_dt_m0)), date(dt))-1 as date_diff_m0,
date_diff('day',max(date(fi_bill_dt_m1)), date(dt))-1 as date_diff_m1,
date_diff('day',max(date(fi_bill_dt_m2)), date(dt))-1 as date_diff_m2,
date_diff('day',max(date(fi_bill_dt_m3)), date(dt))-1 as date_diff_m3,
max(pd_mix_cd) as pd_mix_cd,
max(pd_mix_nm) as pd_mix_nm,
case when max(pd_mix_nm) = 'TV' then 'a.tv_only'
    when max(pd_mix_nm) like '%TV%' then 'b.tv_bundle'
    else 'c.other' end as tv_bundle_type,
max(pd_bb_accs_media) as pd_bb_accs_media,
max(pd_TV_accs_media) as pd_TV_accs_media,
max(pd_VO_accs_media) as pd_VO_accs_media, 
max(act_acct_inst_dt) as act_acct_inst_dt,
max(act_cust_strt_dt) as act_cust_strt_dt,
max(act_cust_typ_nm) as act_cust_typ_nm,
max(NR_FDP) as NR_FDP,
max(date(fi_bill_dt_m0)) as fi_bill_dt_m0,
max(date(fi_bill_dt_m1)) as fi_bill_dt_m1,
max(date(fi_bill_dt_m2)) as fi_bill_dt_m2,
max(date(fi_bill_dt_m3)) as fi_bill_dt_m3
--max(fi_bill_due_dt_m0) as fi_bill_due_dt_m0,
--max(fi_bill_due_dt_m1) as fi_bill_due_dt_m1,
--max(fi_bill_due_dt_m2) as fi_bill_due_dt_m2
,pd_bb_prod_cd,pd_tv_prod_cd,pd_vo_prod_cd
from "db-analytics-prod"."fixed_cwp"
where act_cust_typ_nm = 'Residencial'
--and date(dt) = date('2022-03-02')
and date(dt) between (select start_date from parameters) and (select end_date from parameters) 
--and (cast(fi_outst_age as int) <=((select day_max from parameters) +10 )or fi_outst_age is null)
group by act_acct_cd, date(dt),pd_bb_prod_cd,pd_tv_prod_cd,pd_vo_prod_cd
--limit 100
),

dna_calc as (
select *,
case when pd_mix_cd is null then 0 else cast(replace(pd_mix_cd,'P','') as int) end as RGUs,
case when DATE_DIFF('day', date(act_cust_strt_dt), date(dt)) <= 183 THEN  'a. 0 to 6 months'
when DATE_DIFF('day', date(act_cust_strt_dt), date(dt)) <= 365 THEN  'b. 7 to 12 months'
when DATE_DIFF('day', date(act_cust_strt_dt), date(dt)) > 365 THEN  'c. More than 12 months'
end as tenure_tier,
Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
When pd_bb_accs_media = 'HFC' Then '2. HFC'
when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
ELSE '3. Copper' END as TECHNOLOGY_PROXY
from dna
    
),

lag_dna as (
select *, 
date_format(date(dt), '%m') as month_dna,  
date_format(date(dt), '%Y%m') as yearmonth_dna,  
extract (day from dt) as day_dna,
case when fi_outst_age is null and (next1_fi_outst_age-prev1_fi_outst_age) = 2 then prev1_fi_outst_age+1
    when fi_outst_age is null and (next2_fi_outst_age-prev2_fi_outst_age) = 4 then prev2_fi_outst_age+2
    when fi_outst_age is null and (next2_fi_outst_age-prev1_fi_outst_age) = 3 then prev1_fi_outst_age+1
    when fi_outst_age is null and (next1_fi_outst_age-prev2_fi_outst_age) = 3 then prev2_fi_outst_age+2
    else fi_outst_age end as fi_outst_age_fix

from (
    select *,
    lag(fi_outst_age) over (partition by act_acct_cd order by dt desc) as next1_fi_outst_age,
    lag(fi_outst_age,2) over (partition by act_acct_cd order by dt desc) as next2_fi_outst_age,
--    lag(fi_outst_age,3) over (partition by act_acct_cd order by dt desc) as next3_fi_outst_age,
--    lag(fi_outst_age,4) over (partition by act_acct_cd order by dt desc) as next4_fi_outst_age,
    lag(fi_outst_age) over (partition by act_acct_cd order by dt) as prev1_fi_outst_age,
    lag(fi_outst_age,2) over (partition by act_acct_cd order by dt) as prev2_fi_outst_age,
    lag(dt) over (partition by act_acct_cd order by dt desc) as next1_dt,
    lag(dt,2) over (partition by act_acct_cd order by dt desc) as next2_dt
    from dna_calc
) 
),

dna_completed as (
select *,
case when fi_outst_age_fix is null and prev1_fi_outst_age is not null then prev1_fi_outst_age + 1 else fi_outst_age_fix end as fi_outst_age_fix2
from lag_dna
),

monthly_snapshot as (
select *,
case when (cast(fi_outst_age as int)<=30 or fi_outst_age is null) then 'a.30_or_less_days_outstanding'
    when (cast(fi_outst_age as int)<=60) then 'b.31_to_60_days_outstanding'
    when (cast(fi_outst_age as int)<=90) then 'c.61_to_90_days_outstanding'
    when (cast(fi_outst_age as int)>90) then 'd.more_than_90_days_outstanding'
    else 'error' end as debt_tranche,
Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
    When pd_bb_accs_media = 'HFC' Then '2. HFC'
    when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
    when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
    when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
    when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' end as TECHNOLOGY_PROXY,
case when pd_mix_cd is null then 0 else cast(replace(pd_mix_cd,'P','') as int) end as RGUs
,(numBB+numTV+numVO) as RGUs_adj
from (select act_acct_cd, case when fi_outst_age_fix2 is null then -1 else cast(fi_outst_age_fix2 as int) end as fi_outst_age, pd_mix_cd,
    pd_bb_accs_media, pd_TV_accs_media, pd_VO_accs_media, act_cust_typ_nm, date(dt) as dt 
    ,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numBB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numVO
    from dna_completed 
    where 
    --date(dt) BETWEEN (select start_date from parameters) and (select end_date from parameters) and (extract(day from date(dt)) = 1)
    date(dt) in (select months from list_dates)
    )
where act_cust_typ_nm = 'Residencial'
)

--select * from dna_completed where dt = date('2022-02-28') and fi_outst_age_fix is null
,juan_approach as(
select
dt, TECHNOLOGY_PROXY,debt_tranche,--count(distinct 
act_acct_cd, --) as num_accounts,sum(
RGUs--) as total_RGUs
,rgus_adj
from monthly_snapshot
where --dt in(date('2022-05-31'),date('2022-07-31'))and 
debt_tranche <>'d.more_than_90_days_outstanding'
--group by dt, TECHNOLOGY_PROXY,debt_tranche order by dt, TECHNOLOGY_PROXY,debt_tranche
)
,FMC_Table AS
( SELECT month,fixedaccount,f_activebom,f_activeeom,b_overdue,fixed_b_maxstart,b_fixedtenure,b_fixed_mrc,b_techflag,b_numrgus,b_mixname_adj,b_mixcode_adj,e_overdue,fixed_e_maxstart,e_fixedtenure,e_fixed_mrc,e_techflag,e_numrgus,e_mixname_adj,e_mixcode_adj,fixedmainmovement,fixedchurntype,fixed_rejoinermonth,waterfall_flag
FROM  "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where Month = date(dt)
)
select distinct * --month,count(distinct fixedaccount) as Users,sum(b_numrgus) as b_rgus,sum(e_numrgus) as e_rgus
--dt,count(distinct act_acct_Cd) as Users,sum(rgus) as RGUs,sum(rgus_adj) as RGUs_adj
from juan_approach j left join fmc_table f on j.act_acct_Cd=f.fixedaccount and date_trunc('month',j.dt)=f.month
--where f.month is null
--where rgus<>e_numrgus
WHERE  fixedchurntype is not null 
--and f.month is not null
--group by 1 order by 1
