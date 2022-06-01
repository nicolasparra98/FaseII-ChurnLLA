WITH FMC_Table AS(
SELECT DISTINCT *
FROM "lla_cco_int_stg"."cwp_sp3_basekpis_dashboardinput_dinamico_rj_V2"
--LIMIT 10
)
-----------Involuntary KPIs Key Fields--------------------------
,Invol_Funnel_Fields AS(
SELECT DISTINCT DATE(DATE_TRUNC('MONTH',d.load_dt)) AS Month,date(d.load_dt) AS Load_dt,DATE(DATE_TRUNC('MONTH',fi_bill_dt_m0)) AS BillMonth,date(fi_bill_dt_m0) as BillDay,d.act_acct_cd,d.fi_outst_age AS DueDays
,CASE WHEN ACT_BLNG_CYCL IN('A','B','C') THEN 15 ELSE 28 END AS FirstOverdueDay
FROM "lla_cco_int_stg"."cwp_fix_union_dna" d
WHERE act_cust_typ_nm = 'Residencial'
)
-------------Cohort Approach-----------------------------------
,Cohort_FirstDayOverdue AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays=FirstOverdueDay THEN act_acct_cd ELSE null END AS Overdue1Day
--,CASE WHEN DueDays=46 THEN act_acct_cd ELSE null END AS SoftDx
--,CASE WHEN DueDays BETWEEN (90-(date_diff('day',date_trunc('Month', date(load_dt)),date_trunc('Month',date(load_dt)) + interval '1' MONTH - interval '1' day))) AND 89 THEN act_acct_cd ELSE null END AS Backlog --1 DIA DEL MES
--,CASE WHEN DueDays=90 THEN act_acct_cd ELSE null END AS HardDx
FROM FMC_Table f LEFT JOIN Invol_Funnel_Fields a ON f.fixedaccount=a.act_acct_cd AND f.month=a.month
--GROUP BY 1,2,3--,4,5,6
)
,Cohort_SoftDx AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays=46 THEN act_acct_cd ELSE null END AS SoftDx
FROM Cohort_FirstDayOverdue f LEFT JOIN Invol_Funnel_Fields b ON f.fixedaccount=b.act_acct_cd AND f.month=b.month
--GROUP BY 1,2,3
)
,Cohort_Backlog AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays BETWEEN (90-(date_diff('day',date_trunc('Month', date(c.load_dt)),date_trunc('Month',date(c.load_dt)) + interval '1' MONTH - interval '1' day))) AND 90 THEN act_acct_cd ELSE null END AS Backlog
FROM Cohort_SoftDx f LEFT JOIN Invol_Funnel_Fields c ON f.fixedaccount=c.act_acct_cd AND f.month=c.month
WHERE date(c.load_dt)=date(date_trunc('month',c.load_dt))
--GROUP BY 1,2,3
)
,Cohort_HardDx AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays=90 THEN act_acct_cd ELSE null END AS HardDx
FROM Cohort_Backlog f LEFT JOIN Invol_Funnel_Fields d ON f.fixedaccount=d.act_acct_cd AND f.month=d.month
--WHERE date(d.load_dt)<>date(date_trunc('month',d.load_dt))
--GROUP BY 1,2,3
)
,Cohort_Flag AS(
SELECT DISTINCT *--,a.Overdue1Day,b.SoftDx,c.Backlog,d.HardDx
FROM Cohort_HardDx
/*
FROM FMC_Table f --LEFT JOIN Cohort_Customers c ON f.fixedaccount=c.act_acct_cd AND f.month=c.month
 LEFT JOIN Cohort_FirstDayOverdue a ON f.fixedaccount=a.act_acct_cd AND f.month=a.month
 LEFT JOIN Cohort_SoftDx b ON f.fixedaccount=b.act_acct_cd AND f.month=b.month
 LEFT JOIN Cohort_Backlog c ON f.fixedaccount=c.act_acct_cd AND f.month=c.month
 LEFT JOIN Cohort_HardDx d ON f.fixedaccount=d.act_acct_cd AND f.month=d.month
 */
--where act_acct_cd='314020850000'
)
/*
select distinct count(distinct finalaccount)--,c.backlog,h.harddx
from cohort_flag --left join cohort_harddx h ON c.fixedaccount=h.act_acct_cd and c.month=h.month
where month=date('2022-02-01') and backlog is not null and harddx is null
--order by c.finalaccount
*/
-----------------Funnel Approach ------------------------------------

SELECT DISTINCT --month,count(distinct BACKLOG)
month,B_FMCSegment,B_FMCType,B_Final_TechFlag,E_FMCSegment,E_FMCType,E_Final_TechFlag,fixedchurnflag,fixedchurntype,fixedchurnsubtype,fixedmainmovement,waterfall_flag,count(distinct fixedaccount) as Users, count(distinct Overdue1Day) as Overdue1Day, count(distinct SoftDx) as SoftDx,count(distinct backlog) AS Backlog,count(distinct harddx) as HardDx
 --,fixedaccount as Users,Overdue1Day,SoftDx,Backlog,HardDx
FROM Cohort_Flag
--WHERE Month=date('2022-02-01') and B_FMCSEGMENT IS NOT NULL --harddx IS NULL AND backlog IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
--order by users
