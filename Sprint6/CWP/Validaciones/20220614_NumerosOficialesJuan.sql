--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."PruebaSoft" AS
-----------------------------------------
-- DIRECT TO SOFT DX QUERY
-----------------------------------------
WITH parameters as (
select date('2022-02-01') as month_analysis,
46 as non_pay_threshold
),

union_dna as (
    select act_acct_cd, fi_outst_age, load_dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,act_cust_typ_nm
    from "db-analytics-prod"."fixed_cwp"
    where act_cust_typ_nm = 'Residencial'
    and date(dt) between ((select month_analysis from parameters ) - interval '6' month) and ((select month_analysis from parameters ) + interval '5' month)
    and (cast(fi_outst_age as bigint) <= 95 or fi_outst_age is null)
    union all
    select act_acct_cd, fi_outst_age, load_dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,act_cust_typ_nm
    from "db-analytics-dev"."dna_fixed_cwp" 
    where act_cust_typ_nm = 'Residencial'
    and date(dt) between ((select month_analysis from parameters ) - interval '6' month) and ((select month_analysis from parameters ) + interval '5' month)
    and (cast(fi_outst_age as bigint) <= 95 or fi_outst_age is null)
),

monthly_inst_accounts as (
SELECT 
    act_acct_cd
from union_dna
WHERE 
    act_cust_typ_nm = 'Residencial'
    and DATE_TRUNC('month',date(act_acct_inst_dt)) = (select month_analysis from parameters)
),

first_bill as (
SELECT act_acct_cd, concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt)) as act_first_bill
FROM
    (select act_acct_cd,
    FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_inst_dt, 
    FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_oldest_unpaid_bill_dt
    from 
        (
        select act_acct_cd, fi_outst_age, load_dt,act_acct_inst_dt,
        case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(load_dt)) as varchar) end as oldest_unpaid_bill_dt
        from union_dna
         WHERE act_cust_typ_nm = 'Residencial'
        and act_acct_cd in (select act_acct_cd from monthly_inst_accounts)
        AND date(load_dt) between ((select month_analysis from parameters ) - interval '6' month) and ((select month_analysis from parameters ) + interval '2' month)
        )
  where oldest_unpaid_bill_dt <> '1900-01-01'
  )
where DATE_TRUNC('month',first_inst_dt) = (select month_analysis from parameters)
group by act_acct_cd
),

max_overdue_first_bill as (
select act_acct_cd, 
min(date(first_oldest_unpaid_bill_dt)) as first_oldest_unpaid_bill_dt,
min(first_inst_dt) as first_inst_dt, min(first_act_cust_strt_dt) as first_act_cust_strt_dt,
concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt))  as act_first_bill,
max(fi_outst_age) as max_fi_outst_age, 
max(date(load_dt)) as max_dt,
case when max(cast(fi_outst_age as int))>=(46) and max(cast(fi_outst_age as int))<(90) then 1 else 0 end as soft_dx_flg,
case when max(cast(fi_outst_age as int))>=(90) then 1 else 0 end as hard_dx_flg
FROM
    (select act_acct_cd,
    FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_oldest_unpaid_bill_dt,
    FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_inst_dt, 
    FIRST_VALUE(date(act_cust_strt_dt)) OVER (PARTITION BY act_acct_cd ORDER BY load_dt) AS first_act_cust_strt_dt,
    fi_outst_age, load_dt, pd_mix_cd
    FROM 
        (
        select act_acct_cd, fi_outst_age, load_dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,
        case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(load_dt)) as varchar) end as oldest_unpaid_bill_dt
        from union_dna
         WHERE act_cust_typ_nm = 'Residencial'
         and act_acct_cd in (select act_acct_cd from monthly_inst_accounts)
         AND date(load_dt) between (select month_analysis from parameters ) and ((select month_analysis from parameters ) + interval '5' month)
        )
    where concat(act_acct_cd,'-',oldest_unpaid_bill_dt) in (select act_first_bill from first_bill)
    )
group by act_acct_cd

)
,final_query as(
select *, (select month_analysis from parameters) as month_analysis,
date_add('day',(select non_pay_threshold from parameters),first_oldest_unpaid_bill_dt) as threshold_pay_date,
case when date_add('day',(46),first_oldest_unpaid_bill_dt)  < current_date then 1 else 0 end as soft_dx_window_completed,
case when date_add('day',(90),first_oldest_unpaid_bill_dt)  < current_date then 1 else 0 end as never_paid_window_completed,
current_date as current_date_analysis
from max_overdue_first_bill
)
select distinct f.month_analysis,f.soft_dx_window_completed,f.soft_dx_flg
--,f.never_paid_window_completed,f.hard_dx_flg
,count(distinct f.act_acct_cd)
from final_query f --inner join "lla_cco_int_stg"."PruebaSoft2" p on f.act_acct_cd=p.act_acct_cd
--where f.month_analysis=date('2022-03-01') --and f.soft_dx_window_completed = 1 
 --and f.soft_dx_flg = 1 --and p.soft_dx_flg=1 
 --and date_trunc('month',first_act_cust_strt_dt)<date('2022-01-01')
group by 1,2,3--,4,5
order by 1,2,3--,4,5

--select count(*) from first_bill--  where act_acct_cd = '196044740000'
