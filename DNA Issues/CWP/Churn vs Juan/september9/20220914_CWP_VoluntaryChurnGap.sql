WITH parameters as (
select
--#####################################################################################
date('2022-07-01') as start_date, 
date('2022-07-31') as end_date, 
90 as non_pay_threshold
--####################################################################################
),

dna as (
select act_acct_cd,
max(first_act_acct_stat) as first_act_acct_stat,
max(last_act_acct_stat) as last_act_acct_stat,
max(first_dt) as first_dt,
max(last_dt) as last_dt,
max(first_fi_outst_age) as first_fi_outst_age,
max(last_fi_outst_age) as last_fi_outst_age,
max(first_pd_mix_cd) as first_pd_mix_cd,
max(last_pd_mix_cd) as last_pd_mix_cd,
max(first_pd_mix_nm) as first_pd_mix_nm,
max(last_pd_mix_nm) as last_pd_mix_nm,
min(start_rgu_number) as start_rgu_number,
min(end_rgu_number) as end_rgu_number,
case when max(last_dt) < (select end_date from parameters) then 
            (case when min(first_pd_mix_cd) = '1P' then 1 when min(first_pd_mix_cd) = '2P' then 2 when min(first_pd_mix_cd) = '3P' then 3 else 0 end)
        else 
            (case when min(first_pd_mix_nm)  LIKE '%BO%' and (min(last_pd_mix_nm) NOT LIKE '%BO%' or min(last_pd_mix_nm) is null) then 1 else 0 end +
            case when min(first_pd_mix_nm)  LIKE '%VO%' and (min(last_pd_mix_nm) NOT LIKE '%VO%' or min(last_pd_mix_nm) is null)then 1 else 0 end +
            case when min(first_pd_mix_nm)  LIKE '%TV%' and (min(last_pd_mix_nm) NOT LIKE '%TV%' or min(last_pd_mix_nm) is null)then 1 else 0 end) end AS net_rgu_loss
,max(act_acct_inst_dt) as act_acct_inst_dt
,max(act_cust_typ_nm) as act_cust_typ_nm
,max(date(act_cust_strt_dt)) as act_cust_strt_dt
,CASE WHEN (min(first_fi_outst_age) <= (select non_pay_threshold from parameters) or min(first_fi_outst_age) is null) AND min(last_fi_outst_age) >= (select non_pay_threshold from parameters) THEN 1 ELSE 0 END as net_inv_churn_flag,

min(TECHNOLOGY_PROXY) as TECHNOLOGY_PROXY
,(select end_date from parameters) as end_date
,case when max(last_dt) < (select end_date from parameters) then 1 else 0 end as prueba
from (
    select act_acct_cd,
    first_value(date(dt)) over (partition by act_acct_cd order by dt) as first_dt,
    first_value(date(dt)) over (partition by act_acct_cd order by dt desc) as last_dt,
    first_value(act_acct_stat) over (partition by act_acct_cd order by dt) as first_act_acct_stat,
    first_value(act_acct_stat) over (partition by act_acct_cd order by dt desc) as last_act_acct_stat,
    first_value(cast(fi_outst_age as int)) over (partition by act_acct_cd order by dt) as first_fi_outst_age,
    first_value(cast(fi_outst_age as int)) over (partition by act_acct_cd order by dt desc) as last_fi_outst_age,
    first_value(pd_mix_cd) over (partition by act_acct_cd order by dt) as first_pd_mix_cd,
    first_value(pd_mix_cd) over (partition by act_acct_cd order by dt desc) as last_pd_mix_cd,
    first_value(pd_mix_nm) over (partition by act_acct_cd order by dt) as first_pd_mix_nm,
    first_value(pd_mix_nm) over (partition by act_acct_cd order by dt desc) as last_pd_mix_nm,
    first_value(case when pd_mix_cd is null then 0 else cast(replace(pd_mix_cd,'P','') as int) end) over (partition by act_acct_cd order by dt) as start_rgu_number,
    first_value(case when pd_mix_cd is null then 0 else cast(replace(pd_mix_cd,'P','') as int) end) over (partition by act_acct_cd order by dt desc) as end_rgu_number,
    Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
    When pd_bb_accs_media = 'HFC' Then '2. HFC'
    when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
    when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
    when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
    when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' END as TECHNOLOGY_PROXY,
    act_acct_inst_dt,
    act_cust_typ_nm
    ,act_cust_strt_dt
    
    from "db-analytics-prod"."fixed_cwp"
    where act_cust_typ_nm = 'Residencial'
    and date(dt) between (select start_date from parameters) and (select end_date from parameters) 
    )
--where (first_fi_outst_age <= (select non_pay_threshold from parameters) or first_fi_outst_age is null) 
group by act_acct_cd
)
--select *
--from dna where act_acct_cd='118020950000'
,

active_rejoiner_class as (
select *,
case when  (first_fi_outst_age <= (select non_pay_threshold from parameters) or first_fi_outst_age is null) then 1 else 0 end as active_flag,
case when  (first_fi_outst_age > (select non_pay_threshold from parameters) and  last_fi_outst_age < (select non_pay_threshold from parameters)) then 1 else 0 end as rejoiner_flag
from dna
)

,vol_panel as (
select account_id_vol_dx, order_id,
max(vo_so_churn_flg) as vo_so_churn_flg, max(tv_so_churn_flg) as tv_so_churn_flg, max(bb_so_churn_flg) as bb_so_churn_flg,
filter(array_sort(array_agg(distinct order_id)), x -> x IS NOT NULL) as list_orders,
filter(array_sort(array_agg(distinct cease_reason_desc)), x -> x IS NOT NULL) as list_cease_reason_desc
from (
    select account_id as account_id_vol_dx, order_id,
    date(order_start_date) as order_start_date,
    order_type, cease_reason_desc, channel_desc, order_status,
    cast(lob_vo_count as double) + cast(lob_tv_count as double) + cast(lob_bb_count as double) as RGUs,
    case when cast(lob_vo_count as double) >0 then 1 else 0 end as vo_so_churn_flg,
    case when cast(lob_tv_count as double) >0 then 1 else 0 end as tv_so_churn_flg,
    case when cast(lob_bb_count as double) >0 then 1 else 0 end as bb_so_churn_flg,
    first_value(date(completed_date)) over (partition by account_id, order_id order by data_creation_timestamp desc) as completed_date,
    first_value(order_status) over (partition by account_id, order_id order by data_creation_timestamp desc) as last_order_status
    FROM "db-stage-dev"."so_hdr_cwp" 
    where date(completed_date) between (select start_date from parameters) and (select end_date from parameters) 
    and order_type = 'DEACTIVATION' and cease_reason_desc not like '%INCUMPLE PAGO%'
    AND order_status = 'COMPLETED'
    --and cast(lob_vo_count as double) + cast(lob_tv_count as double) + cast(lob_bb_count as double)>0
    )
group by account_id_vol_dx, order_id
order by account_id_vol_dx, order_id
),

user_panel_so as (
    select account_id_vol_dx,
    case when count(*) > 0 then 1 else 0 end as vol_so_flag,
    case when max(bb_so_churn_flg)>0 then 1 else 0 end as vol_lob_bb_count,
    case when max(tv_so_churn_flg)>0 then 1 else 0 end as vol_lob_tv_count,
    case when max(vo_so_churn_flg)>0 then 1 else 0 end as vol_lob_vo_count
    from vol_panel 
    group by account_id_vol_dx
),


join_so_panel as (
select a.*, b.*
--from dna a
from active_rejoiner_class a 
left join user_panel_so b 
on cast(b.account_id_vol_dx as varchar)= a.act_acct_cd
where a.active_flag = 1 or a.rejoiner_flag = 1
),

churn_cat as (
select *,
case when net_inv_churn_flag = 0 and vol_so_flag = 1  and net_rgu_loss > 0 then 1 else 0 end as vol_churn_flag,
case when net_inv_churn_flag = 0 and (vol_so_flag = 0 or vol_so_flag is null) and (last_dt=(select end_date from parameters) and net_rgu_loss > 0) then 1 else 0 end as other_partial_churn_flag,
case when net_inv_churn_flag=0 and (vol_so_flag = 0 or vol_so_flag is null) and  last_dt<(select end_date from parameters )  then 1 else 0 end as early_dx_flag
from join_so_panel
),

rgu_count as (
select *,
case when rejoiner_flag = 1 then -end_rgu_number ELSE 0 end as rejoiners_rgu,
case when net_inv_churn_flag = 1 then start_rgu_number ELSE 0 end as net_inv_churn_rgu,
case when vol_churn_flag = 1 then net_rgu_loss else 0 end as vol_churn_rgu,
case when other_partial_churn_flag = 1 then net_rgu_loss else 0 end as other_partial_churn_rgu,
case when early_dx_flag = 1 then net_rgu_loss else 0 end as early_dx_churn_rgu
from churn_cat
)
,panel_juan as(
select * 
from rgu_count
)
/*
select * from(
select distinct date_trunc('month',act_cust_strt_dt),count(distinct act_acct_cd),sum(CASE WHEN rejoiner_flag = 1 THEN rejoiners_rgu ELSE 0 END)  as rejoiners_rgu
from panel_juan group by 1 order by 1) where rejoiners_rgu<0
*/

/*,summary_churn as (
  select --TECHNOLOGY_PROXY,
  count(*) AS monthly_base,
  sum(vol_churn_flag) as vol_churners,
  SUM(net_inv_churn_flag) AS net_inv_churners,
  SUM(other_partial_churn_flag) AS other_partial_churners,
  SUM(early_dx_flag) as early_dx_churners,
  SUM(start_rgu_number) AS monthly_base_rgu,
  sum(case when vol_churn_flag=1 then vol_churn_rgu else 0 end) as vol_churners_rgu,
  SUM(CASE WHEN net_inv_churn_flag = 1 THEN net_inv_churn_rgu ELSE 0 END) AS net_inv_churners_rgu,  
  SUM(case when other_partial_churn_flag=1 then net_rgu_loss else 0 end) AS other_partial_churners_rgu,
  sum(CASE WHEN early_dx_flag = 1 THEN early_dx_churn_rgu ELSE 0 END)  as early_dx_churners_rgu,
  min(first_dt) as first_dt,
  max(last_dt) as last_dt
  , sum(CASE WHEN rejoiner_flag = 1 THEN rejoiners_rgu ELSE 0 END)  as rejoiners_rgu
  from panel_juan
  --group by TECHNOLOGY_PROXY
  --ORDER BY TECHNOLOGY_PROXY*/
--/*
,fmc_table as(
SELECT distinct * --month,count(distinct fixedaccount),SUM(e_numrgus)
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where month=date(dt) and month in(date('2022-07-01'),date('2022-08-01'))
--group by 1 order by 1
)
--*/
--,voluntary_gap as(
select distinct --* 
month,fixedmainmovement
,fixed_rejoinermonth,rejoinerfmcflag,waterfall_flag,f_activebom
,count(distinct act_acct_cd),sum(CASE WHEN rejoiner_flag = 1 THEN rejoiners_rgu ELSE 0 END)
from panel_juan j inner join fmc_table f on j.act_acct_cd=f.fixedaccount
--where vol_churn_flag=1 and (fixedchurntype is null) --and net_inv_churn_flag=0
where rejoiners_rgu<0
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
--)
/*,dna_invol_check as(
select distinct date(d.dt),d.act_acct_cd,d.fi_outst_age,d.pd_mix_cd,b_numrgus,net_inv_churn_flag
from "db-analytics-prod"."fixed_cwp" d inner join voluntary_gap v on d.act_acct_cd=i.act_acct_Cd
where date(d.dt) between date('2022-06-30') and date('2022-07-31')
order by 2,1 asc
--)*/
