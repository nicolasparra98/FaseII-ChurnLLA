-----------------------------------------
-- DIRECT TO SOFT DX QUERY
-----------------------------------------
--WITH parameters as (
--select date('2022-03-01') as month_analysis,
--46 as non_pay_threshold
--),
with
union_dna as (
    select act_acct_cd, fi_outst_age, load_dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,act_cust_typ_nm,date_trunc('month',date(load_dt)) as month
    from "db-analytics-prod"."fixed_cwp"
    where act_cust_typ_nm = 'Residencial'
    and (cast(fi_outst_age as bigint) <= 95 or fi_outst_age is null)
    and date(load_dt) between (Date_trunc('Month', date(act_acct_inst_dt)) - interval '6' month) and (Date_trunc('Month', date(act_acct_inst_dt)) + interval '5' month)
    union all
    select act_acct_cd, fi_outst_age, load_dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,act_cust_typ_nm,date_trunc('month',date(load_dt)) as month
    from "db-analytics-dev"."dna_fixed_cwp" 
    where act_cust_typ_nm = 'Residencial'
    and (cast(fi_outst_age as bigint) <= 95 or fi_outst_age is null)
    and date(load_dt) between (Date_trunc('Month', date(act_acct_inst_dt)) - interval '6' month) and (Date_trunc('Month', date(act_acct_inst_dt)) + interval '5' month)
)
,monthly_inst_accounts as (
SELECT 
    act_acct_cd,DATE_TRUNC('month',date(act_acct_inst_dt)) as Month
from union_dna
WHERE 
    act_cust_typ_nm = 'Residencial'
    and DATE_TRUNC('month',date(act_acct_inst_dt)) = month
),

first_bill as (
SELECT act_acct_cd, concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt)) as act_first_bill,DATE_TRUNC('month',date(first_inst_dt)) as Month
FROM
    (select act_acct_cd,
    FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_inst_dt, 
    FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_oldest_unpaid_bill_dt
    from 
        (
        select u.act_acct_cd, u.fi_outst_age,u.load_dt,u.act_acct_inst_dt
        ,case when u.fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(u.fi_outst_age as int),date(u.load_dt)) as varchar) end as oldest_unpaid_bill_dt
        from union_dna u inner join monthly_inst_accounts m on u.act_acct_cd=m.act_acct_cd and u.month=m.month
         WHERE act_cust_typ_nm = 'Residencial'
        --and act_acct_cd in (select act_acct_cd from monthly_inst_accounts)
                AND date(load_dt) between (Date_trunc('Month', date(act_acct_inst_dt)) - interval '6' month) and (Date_trunc('Month', date(act_acct_inst_dt)) + interval '2' month)
        )
  where oldest_unpaid_bill_dt <> '1900-01-01'
  )
--where DATE_TRUNC('month',first_inst_dt) = month
group by act_acct_cd,DATE_TRUNC('month',date(first_inst_dt))
),

max_overdue_first_bill as (
select act_acct_cd, date_trunc('month',date(min(first_inst_dt))) as month,
min(date(first_oldest_unpaid_bill_dt)) as first_oldest_unpaid_bill_dt,
min(first_inst_dt) as first_inst_dt, min(first_act_cust_strt_dt) as first_act_cust_strt_dt,
concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt))  as act_first_bill,
max(fi_outst_age) as max_fi_outst_age, 
max(date(load_dt)) as max_dt,
case when max(cast(fi_outst_age as int))>=(46) then 1 else 0 end as soft_dx_flg,
case when max(cast(fi_outst_age as int))>=(90) then 1 else 0 end as hard_dx_flg
FROM
    (select act_acct_cd,
    FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_oldest_unpaid_bill_dt,
    FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_inst_dt, 
    FIRST_VALUE(date(act_cust_strt_dt)) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_act_cust_strt_dt,
    fi_outst_age, load_dt, pd_mix_cd
    FROM 
        (
        select act_acct_cd, fi_outst_age,load_dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,
        case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(load_dt)) as varchar) end as oldest_unpaid_bill_dt
        from union_dna
         WHERE act_cust_typ_nm = 'Residencial'
         and act_acct_cd in (select act_acct_cd from monthly_inst_accounts)
         and date(load_dt) between Date_trunc('Month', date(act_acct_inst_dt)) and (Date_trunc('Month', date(act_acct_inst_dt)) + interval '5' month)
        )
    where concat(act_acct_cd,'-',oldest_unpaid_bill_dt) in (select act_first_bill from first_bill)
    )
group by act_acct_cd

)
--/*
,final_query as(
select *, 
date_add('day',46,first_oldest_unpaid_bill_dt) as threshold_pay_date,
case when date_add('day',(46),first_oldest_unpaid_bill_dt)  < current_date then 1 else 0 end as soft_dx_window_completed,
case when date_add('day',(90),first_oldest_unpaid_bill_dt)  < current_date then 1 else 0 end as hard_dx_window_completed,
current_date as current_date_analysis
from max_overdue_first_bill
--*/
)
select distinct month
,soft_dx_window_completed,soft_dx_flg
--,hard_dx_window_completed,hard_dx_flg
,count(distinct act_acct_cd)
from final_query
group by 1,2,3--,4,5
order by 1,2,3--,4,5
