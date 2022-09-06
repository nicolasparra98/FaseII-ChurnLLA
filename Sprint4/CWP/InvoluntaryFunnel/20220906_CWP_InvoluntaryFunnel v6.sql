WITH FMC_Table AS(
SELECT DISTINCT *
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
where month=date(dt) --limit 20
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
--/*
SELECT DISTINCT --month,count(distinct BACKLOG)
month
,B_FMCSegment,B_FMCType,B_Final_TechFlag,E_FMCSegment,E_FMCType,E_Final_TechFlag,fixedchurnflag,fixedchurntype,fixedchurnsubtype,fixedmainmovement,waterfall_flag
,count(distinct fixedaccount) as Users
,count(distinct e_bb) as Total_BB
,count(distinct e_tv) as Total_TV
,count(distinct e_vo) as Total_VO
,count(distinct Overdue1Day) as Overdue1Day, count(distinct SoftDx) as SoftDx,count(distinct backlog) AS Backlog,count(distinct harddx) as HardDx, count(distinct Overdue1Day_BB) as Overdue1Day_BB, count(distinct SoftDx_BB) as SoftDx_BB,count(distinct backlog_BB) AS Backlog_BB,count(distinct harddx_BB) as HardDx_BB, count(distinct Overdue1Day_TV) as Overdue1Day_TV, count(distinct SoftDx_TV) as SoftDx_TV,count(distinct backlog_TV) AS Backlog_TV,count(distinct harddx_TV) as HardDx_TV, count(distinct Overdue1Day_VO) as Overdue1Day_VO, count(distinct SoftDx_VO) as SoftDx_VO,count(distinct backlog_VO) AS Backlog_VO,count(distinct harddx_VO) as HardDx_VO
 --,fixedaccount as Users,Overdue1Day,SoftDx,Backlog,HardDx
FROM Cohort_Flag
where month=date(dt)
--WHERE Month=date('2022-02-01') and harddx IS NULL AND backlog IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
--order by users
order by 1
