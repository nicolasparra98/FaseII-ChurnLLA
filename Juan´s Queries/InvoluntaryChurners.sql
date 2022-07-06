WITH parameters as (
select date('2022-02-01') as start_date,
date('2022-06-30') as end_date,
90 as non_pay_threshold
),

union_dna as (
    select act_acct_cd, fi_outst_age, date(dt) as dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,act_cust_typ_nm,
    case when pd_mix_cd is null then 0 else cast(replace(pd_mix_cd,'P','') as int) end as RGUs
    from "db-analytics-prod"."fixed_cwp"
    where act_cust_typ_nm = 'Residencial'
    and date(dt) between (select start_date from parameters) and (select end_date from parameters) 
    --and (cast(fi_outst_age as bigint) <= 400 or fi_outst_age is null)
    --union all
    union distinct
    select act_acct_cd, fi_outst_age, date(dt) as dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,act_cust_typ_nm,
    case when pd_mix_cd is null then 0 else cast(replace(pd_mix_cd,'P','') as int) end as RGUs
    from "db-analytics-dev"."dna_fixed_cwp" 
    where act_cust_typ_nm = 'Residencial'
    and date(dt) between (select start_date from parameters) and (select end_date from parameters) 
    --and (cast(fi_outst_age as bigint) <= 400 or fi_outst_age is null)
),

lag_dna as (
select *, date_trunc('month', date(dt)) month_dt, 
Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
    When pd_bb_accs_media = 'HFC' Then '2. HFC'
    when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
    when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
    when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
    when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' end as tech_service,
case when (fi_outst_age is null and (next1_fi_outst_age> (select non_pay_threshold from parameters) or next2_fi_outst_age> (select non_pay_threshold from parameters) +1 ) or fi_outst_age> (select non_pay_threshold from parameters)) then 1 else 0 end as exclude,--
case when (fi_outst_age = (select non_pay_threshold from parameters) or 
        (next1_fi_outst_age > (select non_pay_threshold from parameters)  and date_trunc('month',proj_day_inv_churn) = date_trunc('month',dt)) or 
        (next2_fi_outst_age > (select non_pay_threshold from parameters)  and date_trunc('month',proj_day_inv_churn) = date_trunc('month',dt)) or 
        (next3_fi_outst_age > (select non_pay_threshold from parameters)  and date_trunc('month',proj_day_inv_churn) = date_trunc('month',dt)) or
        (next4_fi_outst_age > (select non_pay_threshold from parameters)  and date_trunc('month',proj_day_inv_churn) = date_trunc('month',dt))) and
        (prev1_fi_outst_age < (select non_pay_threshold from parameters) or prev2_fi_outst_age < (select non_pay_threshold from parameters)) then 1 else 0 end as inv_churn_flg

from (select *,
        lag(fi_outst_age) over (partition by act_acct_cd order by dt desc) as next1_fi_outst_age,
        lag(fi_outst_age,2) over (partition by act_acct_cd order by dt desc) as next2_fi_outst_age,
        lag(fi_outst_age,3) over (partition by act_acct_cd order by dt desc) as next3_fi_outst_age,
        lag(fi_outst_age,4) over (partition by act_acct_cd order by dt desc) as next4_fi_outst_age,
        lag(fi_outst_age) over (partition by act_acct_cd order by dt) as prev1_fi_outst_age,
        lag(fi_outst_age,2) over (partition by act_acct_cd order by dt) as prev2_fi_outst_age,
        lag(dt) over (partition by act_acct_cd order by dt desc) as next1_dt,
        lag(dt,2) over (partition by act_acct_cd order by dt desc) as next2_dt,
        lag(dt,3) over (partition by act_acct_cd order by dt desc) as next3_dt,
        lag(dt,4) over (partition by act_acct_cd order by dt desc) as next4_dt,
        date_add('day',(select non_pay_threshold from parameters)-fi_outst_age, date(dt)) as proj_day_inv_churn
        from union_dna) 
),
--/*

user_panel as (
select
month_dt, act_acct_cd,
max(RGUs) as RGUs,
max(inv_churn_flg) as inv_churn_flg,
min(tech_service) as tech_service,
max(day_inv_churn) as day_inv_churn
from (select *, 
    case when inv_churn_flg = 1 then proj_day_inv_churn else null end as day_inv_churn
    from lag_dna)
where exclude <> 1 
group by month_dt, act_acct_cd
),

summary as (
select  month_dt, 
count(distinct act_acct_cd) as total_accounts,
sum(RGUs) as total_RGUs,
sum(case when inv_churn_flg = 1 then 1 else 0 end) as inv_churners,
sum(case when inv_churn_flg = 1 then RGUs else 0 end) as inv_churners_RGUs
from user_panel
group by month_dt order by  month_dt
)
--*/
--select  * from lag_dna where act_acct_cd in (select act_acct_cd from lag_dna where exclude = 1)  order by act_acct_cd, dt
select * from summary
--select * from user_panel where inv_churn_flg = 1
--select * from lag_dna where act_acct_cd = '803537310000'
--and act_acct_cd = '124040530000'
