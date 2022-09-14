WITH parameters as (
select
--#####################################################################################
date('2022-08-01') as start_date, 
date('2022-08-31') as end_date, 
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
            (case when min(first_pd_mix_nm)  LIKE '%BB%' and (min(last_pd_mix_nm) NOT LIKE '%BB%' or min(last_pd_mix_nm) is null) then 1 else 0 end +
            case when min(first_pd_mix_nm)  LIKE '%VO%' and (min(last_pd_mix_nm) NOT LIKE '%VO%' or min(last_pd_mix_nm) is null)then 1 else 0 end +
            case when min(first_pd_mix_nm)  LIKE '%TV%' and (min(last_pd_mix_nm) NOT LIKE '%TV%' or min(last_pd_mix_nm) is null)then 1 else 0 end) end AS net_rgu_loss,
max(act_acct_inst_dt) as act_acct_inst_dt,
max(act_cust_typ_nm) as act_cust_typ_nm,
CASE WHEN (min(first_fi_outst_age) <= (select non_pay_threshold from parameters) or min(first_fi_outst_age) is null) AND min(last_fi_outst_age) >= (select non_pay_threshold from parameters) THEN 1 ELSE 0 END as net_inv_churn_flag,

min(TECHNOLOGY_PROXY) as TECHNOLOGY_PROXY
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
    
    from "db-analytics-prod"."fixed_cwp"
    where act_cust_typ_nm = 'Residencial'
    and date(dt) between (select start_date from parameters) and (select end_date from parameters) 
    )
--where (first_fi_outst_age <= (select non_pay_threshold from parameters) or first_fi_outst_age is null) 
group by act_acct_cd
),

active_rejoiner_class as (
select *,
case when  (first_fi_outst_age <= (select non_pay_threshold from parameters) or first_fi_outst_age is null) then 1 else 0 end as active_flag,
case when  ((first_fi_outst_age > (select non_pay_threshold from parameters)) and  (first_fi_outst_age < (select non_pay_threshold from parameters) + 90)

--last_fi_outst_age < (select non_pay_threshold from parameters)) then 1 else 0 end as rejoiner_flag
and (last_fi_outst_age < (select non_pay_threshold from parameters) or last_fi_outst_age is null)) then 1 else 0 end as rejoiner_flag
from dna
),

vol_panel as (
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
case when rejoiner_flag = 1 then -start_rgu_number ELSE 0 end as rejoiners_rgu,
case when net_inv_churn_flag = 1 then start_rgu_number ELSE 0 end as net_inv_churn_rgu,
case when vol_churn_flag = 1 then net_rgu_loss else 0 end as vol_churn_rgu,
case when other_partial_churn_flag = 1 then net_rgu_loss else 0 end as other_partial_churn_rgu,
case when early_dx_flag = 1 then net_rgu_loss else 0 end as early_dx_churn_rgu
from churn_cat
where (rejoiner_flag = 0 or  (rejoiner_flag = 1 and early_dx_flag = 0))

),

summary_churn as (
  select TECHNOLOGY_PROXY,
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
  from rgu_count
  group by TECHNOLOGY_PROXY
  ORDER BY TECHNOLOGY_PROXY
)
,fmc_table as(
SELECT distinct * 
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where month=date(dt) and month=date('2022-08-01')
)
,panel_juan as(
select * from rgu_count 
)
,voluntary_gap as(
select distinct * 
--month,vol_churn_flag,fixedmainmovement,fixedchurntype,count(distinct act_acct_cd) as juan_users,sum(case when vol_churn_flag=1 then vol_churn_rgu else 0 end) as vol_churners_rgu,SUM(other_partial_churn_flag) AS other_partial_churners,sum(case when fixedchurntype='1. Fixed Voluntary Churner' then b_numrgus else 0 end) as vol_churn_oval,sum(case when fixedmainmovement='3.Downsell' then (COALESCE(B_NUMRGUS,0) - coalesce(E_numrgus,0)) else 0 end) as partial_churn_oval
from panel_juan j left join fmc_table f on j.act_acct_cd=f.fixedaccount
--where month is null and vol_churn_flag=1
where fixedchurntype='3. Fixed 0P Churner' and vol_churn_flag=1
--group by 1,2,3,4 order by 1,2,3,4
)
,so as(
select distinct account_id,date(completed_date) as completed_date,order_type,order_status,CASE 
 WHEN cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91') THEN 'Voluntario'
 WHEN cease_reason_code IN('2','74') THEN 'Involuntario'
 WHEN (cease_reason_code = '9' AND cease_reason_desc='CAMBIO DE TECNOLOGIA') OR (cease_reason_code IN('32','44','55','72')) THEN 'Migracion'
 WHEN cease_reason_code = '9' AND cease_reason_desc<>'CAMBIO DE TECNOLOGIA' THEN 'Voluntario'
ELSE NULL END AS DxType
from "db-stage-dev"."so_hdr_cwp"
where order_type = 'DEACTIVATION' AND order_status = 'COMPLETED' and date_trunc('month',date(completed_date))=date('2022-08-01')
)
select s.*
from so s inner join voluntary_gap v on cast(account_id as varchar)=act_acct_cd
order by 1,2


/*select distinct first_dt,last_dt,first_fi_outst_age,last_pd_mix_cd,count(distinct act_acct_cd) as users, sum(vol_churn_rgu) as vol_rgu
from voluntary_gap
group by 1,2,3,4 order by 1,2,3,4*/
/*select distinct date(d.dt),d.act_acct_cd,d.fi_outst_age,d.pd_mix_cd,vol_churn_flag,vol_churn_rgu,net_inv_churn_flag
from "db-analytics-prod"."fixed_cwp" d inner join voluntary_gap i on d.act_acct_cd=i.act_acct_Cd
where date(d.dt) between date('2022-07-31') and date('2022-08-31')
order by 2,1 asc*/
