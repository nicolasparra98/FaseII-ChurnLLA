WITH FMC_Table AS(
SELECT DISTINCT *
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
)
-----------Involuntary KPIs Key Fields--------------------------
,Invol_Funnel_Fields AS(
select *
,first_value(LagDueDay_feb) over(partition by act_acct_cd,DATE(DATE_TRUNC('MONTH',dt)) order by date(dt) desc) as LastDueDay_feb
from(SELECT DISTINCT DATE(DATE_TRUNC('MONTH',date(d.dt))) AS Month,date(d.dt) AS dt,DATE(DATE_TRUNC('MONTH',fi_bill_dt_m0)) AS BillMonth,date(fi_bill_dt_m0) as BillDay,d.act_acct_cd,d.fi_outst_age AS DueDays
,CASE WHEN ACT_BLNG_CYCL IN('A','B','C') THEN 15 ELSE 28 END AS FirstOverdueDay
,case when DATE(DATE_TRUNC('MONTH',date(d.dt)))=date('2022-03-01') then date('2022-03-02') else DATE(DATE_TRUNC('MONTH',date(d.dt))) end as Backlog_Date
,first_value(fi_outst_age) over(partition by act_acct_cd,DATE(DATE_TRUNC('MONTH',date(d.dt))) order by date(dt) desc) as LastDueDay
,lag(fi_outst_age) over(partition by act_acct_cd order by date(dt) asc) as LagDueDay_feb
FROM "db-analytics-prod"."fixed_cwp" d
WHERE act_cust_typ_nm = 'Residencial')
)
-------------Cohort Approach-----------------------------------
,Cohort_FirstDayOverdue AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays=FirstOverdueDay THEN act_acct_cd ELSE null END AS Overdue1Day
FROM FMC_Table f LEFT JOIN Invol_Funnel_Fields a ON f.fixedaccount=a.act_acct_cd AND f.month=a.month
)
,Cohort_SoftDx AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays=46 THEN act_acct_cd ELSE null END AS SoftDx
FROM Cohort_FirstDayOverdue f LEFT JOIN Invol_Funnel_Fields b ON f.fixedaccount=b.act_acct_cd AND f.month=b.month
)
,Cohort_Backlog AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays BETWEEN (90-(date_diff('day',date_trunc('Month', date(c.dt)),date_trunc('Month',date(c.dt)) + interval '1' MONTH - interval '1' day))) AND 90 THEN act_acct_cd ELSE null END AS Backlog
FROM Cohort_SoftDx f LEFT JOIN Invol_Funnel_Fields c ON f.fixedaccount=c.act_acct_cd AND f.month=c.month
WHERE date(c.dt)=c.backlog_date
--date(date_trunc('month',c.dt))
)
,Cohort_HardDx AS(
SELECT DISTINCT f.*
,CASE WHEN f.month>date('2022-02-01') and DueDays>=90 and lastdueday>=90 THEN backlog 
      WHEN f.month=date('2022-02-01') and DueDays>=90 and lastdueday_feb>=90 THEN backlog 
ELSE null END AS HardDx
FROM Cohort_Backlog f LEFT JOIN Invol_Funnel_Fields d ON f.fixedaccount=d.act_acct_cd AND f.month=d.month
)
-----------------------RGUS------------------------------------------
--BB
,Cohort_All_BB AS(
SELECT DISTINCT f.*
,CASE WHEN Overdue1day IS NOT NULL AND E_BB IS NOT NULL THEN overdue1day ELSE null END AS Overdue1Day_BB
,CASE WHEN SoftDx IS NOT NULL AND E_BB IS NOT NULL THEN SoftDx ELSE null END AS SoftDx_BB
,CASE WHEN Backlog IS NOT NULL AND B_BB IS NOT NULL THEN Backlog ELSE null END AS Backlog_BB
,CASE WHEN HardDx IS NOT NULL AND B_BB IS NOT NULL THEN HardDx ELSE null END AS HardDx_BB
FROM Cohort_HardDx f 
)
,Cohort_All_TV AS(
SELECT DISTINCT f.*
,CASE WHEN Overdue1day IS NOT NULL AND E_TV IS NOT NULL THEN overdue1day ELSE null END AS Overdue1Day_TV
,CASE WHEN SoftDx IS NOT NULL AND E_TV IS NOT NULL THEN SoftDx ELSE null END AS SoftDx_TV
,CASE WHEN Backlog IS NOT NULL AND B_TV IS NOT NULL THEN Backlog ELSE null END AS Backlog_TV
,CASE WHEN HardDx IS NOT NULL AND B_TV IS NOT NULL THEN HardDx ELSE null END AS HardDx_TV
FROM Cohort_All_BB f 
)
,Cohort_All_VO AS(
SELECT DISTINCT f.*
,CASE WHEN Overdue1day IS NOT NULL AND E_VO IS NOT NULL THEN overdue1day ELSE null END AS Overdue1Day_VO
,CASE WHEN SoftDx IS NOT NULL AND E_VO IS NOT NULL THEN SoftDx ELSE null END AS SoftDx_VO
,CASE WHEN Backlog IS NOT NULL AND B_VO IS NOT NULL THEN Backlog ELSE null END AS Backlog_VO
,CASE WHEN HardDx IS NOT NULL AND B_VO IS NOT NULL THEN HardDx ELSE null END AS HardDx_VO
FROM Cohort_All_TV f 
)
,Cohort_Flag AS(
SELECT DISTINCT *
FROM Cohort_All_VO
)
/*
SELECT DISTINCT --month,count(distinct BACKLOG)
month
--,B_FMCSegment,B_FMCType,B_Final_TechFlag,E_FMCSegment,E_FMCType,E_Final_TechFlag,fixedchurnflag,fixedchurntype,fixedchurnsubtype,fixedmainmovement,waterfall_flag
,count(distinct fixedaccount) as Users, count(distinct Overdue1Day) as Overdue1Day, count(distinct SoftDx) as SoftDx,count(distinct backlog) AS Backlog,count(distinct harddx) as HardDx, count(distinct Overdue1Day_BB) as Overdue1Day_BB, count(distinct SoftDx_BB) as SoftDx_BB,count(distinct backlog_BB) AS Backlog_BB,count(distinct harddx_BB) as HardDx_BB, count(distinct Overdue1Day_TV) as Overdue1Day_TV, count(distinct SoftDx_TV) as SoftDx_TV,count(distinct backlog_TV) AS Backlog_TV,count(distinct harddx_TV) as HardDx_TV, count(distinct Overdue1Day_VO) as Overdue1Day_VO, count(distinct SoftDx_VO) as SoftDx_VO,count(distinct backlog_VO) AS Backlog_VO,count(distinct harddx_VO) as HardDx_VO
 --,fixedaccount as Users,Overdue1Day,SoftDx,Backlog,HardDx
FROM Cohort_Flag
where month=date(dt)
--WHERE Month=date('2022-02-01') and harddx IS NULL AND backlog IS NOT NULL
GROUP BY 1--,2,3,4,5,6,7,8,9,10,11,12
--order by users
order by 1
*/
-------------------------CARLOS--------------------------------------
,parameters as (
select
    --##############################################################
    --### Change Date in this line to define paying period #########
    date('2022-07-01') as start_date,
    date('2022-07-31') as end_date,
    90 as dx_threshold,
    15 as overdue_threshold_Billing_Cycle_ABC,
    28 as overdue_threshold_Billing_Cycle_D_to_K
    --##############################################################
),

consolidated_dna as (
     select * from 
         (select act_acct_cd, pd_mix_cd, act_cust_typ_nm, fi_outst_age, act_blng_cycl,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,cast(dt as date) as dt  
         from "db-analytics-dev"."dna_fixed_cwp") where dt < date('2022-02-02')
     union all
     select act_acct_cd, pd_mix_cd, act_cust_typ_nm, fi_outst_age, act_blng_cycl,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,cast(dt as date) as dt 
     from "db-analytics-prod"."fixed_cwp" where cast(dt as date) >= date('2022-02-02') 
),

clean_up_previous_overdue as (
    select distinct(act_acct_cd) from consolidated_dna
    where (date(dt)) = (select start_date from parameters) and (cast(fi_outst_age as int) >90)
),

candidates_for_dx as (
    select act_acct_cd,
        -- max(case when pd_mix_cd is null then 0 else cast(replace(pd_mix_cd,'P','') as int) end) as RGUs,
        case when max(pd_bb_prod_nm) is not null then 1 else 0 end as RGU_BB,
        case when max(pd_tv_prod_nm) is not null then 1 else 0 end as RGU_TV,
        case when max(pd_vo_prod_nm) is not null then 1 else 0 end as RGU_VO,   
        min(fi_outst_age) as fi_outst_age
        from consolidated_dna
        where act_cust_typ_nm = 'Residencial'
        and date(dt) = (select start_date from parameters)
        and cast(fi_outst_age as int) between ((select dx_threshold from parameters)-date_diff('day',(select start_date from parameters),(select end_date from parameters))) and ((select dx_threshold from parameters)-1)
        group by act_acct_cd
)
,churn_acct as(select act_acct_cd,
                max(case when pd_mix_cd is null then 0 else cast(replace(pd_mix_cd,'P','') as int) end) as RGUs,
                case when max(pd_bb_prod_nm) is not null then 1 else 0 end as RGU_BB,
                case when max(pd_tv_prod_nm) is not null then 1 else 0 end as RGU_TV,
                case when max(pd_vo_prod_nm) is not null then 1 else 0 end as RGU_VO,
                min(dt) as dt
                from     
                (select 
                    act_acct_cd,
                    first_value(cast(fi_outst_age as int)) over(partition by act_acct_cd order by dt desc) as last_fi_outst_age,
                    first_value(cast(fi_outst_age as int)) over(partition by act_acct_cd order by dt) as first_fi_outst_age,
                    pd_mix_cd,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm, dt
                    from consolidated_dna 
                    where act_cust_typ_nm = 'Residencial'
                    and date(dt) between (select start_date from parameters) and (select end_date from parameters)
                    --- Count as churner only if not DX at period start & was a viable candidate (dismisses "rebirthers") --- 
                    and act_acct_cd not in (select * from clean_up_previous_overdue)
                    and act_acct_cd in (select act_acct_cd from candidates_for_dx)
                )
                --- Excluding early rejoiners ---
            where first_fi_outst_age <= (select dx_threshold from parameters) and last_fi_outst_age>=(select dx_threshold from parameters)   
            group by act_acct_cd)
--------------------------------------------------------------------------------------borrar
/*
select l.*
from cohort_flag l left join candidates_for_dx c on l.backlog=c.act_acct_cd
where l.month=date('2022-06-01')
and c.act_acct_cd is null
and l.backlog is not null
*/
--/*
select *
from cohort_flag l left join churn_acct c on l.harddx=c.act_acct_cd
where l.month=date('2022-07-01')
and c.act_acct_cd is null
and l.harddx is not null
--*/
