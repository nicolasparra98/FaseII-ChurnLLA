with weeklyTable as(
select fixed_month,fixed_monthweek,fixed_account,fixed_cust_cd,f_contactphone1,f_contactphone2,f_contactphone3,activebom,activeeom,b_active_flag,e_active_flag,bow,b_date,b_tech_type,b_mixcode,b_mixcode_adj,b_mixname,b_mixname_adj,b_prodbbname,b_prodtvname,b_prodvoname,bb_rgu_bom,tv_rgu_bom,vo_rgu_bom,b_numrgus,b_bundlecode,b_bundlename,b_mrc,b_outstage,b_mrcadj,b_mrcbb,b_mrctv,b_mrcvo,b_maxstart,b_tenuredays,b_fixedtenuresegment,eow,e_date,e_tech_type,e_mixcode,e_mixcode_adj,e_mixname,e_mixname_adj,e_prodbbname,e_prodtvname,e_prodvoname,bb_rgu_eom,tv_rgu_eom,vo_rgu_eom,e_numrgus,e_bundlecode,e_bundlename,e_mrc,e_outstage,e_mrcadj,e_mrcbb,e_mrctv,e_mrcvo,e_maxstart,e_tenuredays,e_fixedtenuresegment,mrcdiff,last_rgus,dif_rgus,mainmovement,fixed_churntype,rejoinerflag,mainchurnflag
,first_value(b_active_flag_adj) over(partition by fixed_account,fixed_month order by fixed_monthweek asc) as b_active_temp,first_value(e_active_flag_adj) over(partition by fixed_account,fixed_month order by fixed_monthweek desc) as e_active_temp
,CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 and b_active_flag_adj='Active' and e_active_flag_adj='Active' THEN '1.SameRGUs' 
WHEN (E_NumRGUs - B_NumRGUs) > 0 and b_active_flag_adj='Active' and e_active_flag_adj='Active' THEN '2.Upsell'
WHEN (E_NumRGUs - B_NumRGUs) < 0 and b_active_flag_adj='Active' and e_active_flag_adj='Active' THEN '3.Downsell'
WHEN ((B_NumRGUs IS NULL or b_active_flag_adj='DRC') AND (e_active_flag_adj='Active' and E_NumRGUs > 0) AND DATE_TRUNC ('MONTH', DATE(E_MaxStart)) = DATE('2022-06-01')) THEN '4.New Customer'
WHEN ((B_NumRGUs IS NULL or b_active_flag_adj='DRC') AND (e_active_flag_adj='Active' and E_NumRGUs > 0) AND DATE_TRUNC ('MONTH', DATE(E_MaxStart)) <> DATE('2022-06-01')) THEN '5.Come Back to Life'
WHEN ((B_NumRGUs > 0 and b_active_flag_adj='Active') AND (E_NumRGUs IS NULL or e_active_flag_adj='DRC')) THEN '6.Null last day'
WHEN (B_NumRGUs IS NULL or b_active_flag_adj='DRC') AND (E_NumRGUs IS NULL or e_active_flag_adj='DRC') THEN '7.Always null'
END AS MainMovement_adj
from(select *,case when b_active_flag is null then 'DRC' else b_active_flag end as b_active_flag_adj,case when e_active_flag is null then 'DRC' else e_active_flag end as e_active_flag_adj
FROM "lla_cco_int_san"."cwc_fix_stg_weekly_churn_input_mayjune_prueba"
)
--where fixed_account in('234295360000','293316330000','308203890000','318379990000')
order by 1,3,2
)
--select *
--from weeklyTable
--where --activeeom=0 and e_active_temp='Active' and 
--fixed_account='17345301'
------------------------Rejoiners-----------------------------------------------------------------
,InactiveUsersMonth AS (
select distinct Fixed_Month AS ExitMonth, Fixed_Account,DATE_ADD('MONTH', 1, Fixed_Month) AS RejoinerMonth
from weeklyTable
where b_active_temp='Active' and e_active_temp='DRC'
--and fixed_account='104317400000'
)
--select distinct exitmonth,count(distinct fixed_account)
--from InactiveUsersMonth
--group by 1 order by 1

,RejoinerMonthPopulation as(
select distinct Fixed_Month,fixed_monthweek,RejoinerPopFlag,Fixed_PRMonth,Fixed_Account,DATE('2022-06-01') AS Month
from( 
select f.*,RejoinerMonth
,case when i.Fixed_Account is not null then 1 else 0 END AS RejoinerPopFlag
,case when RejoinerMonth>= DATE('2022-06-01') and RejoinerMonth<=date_add('month', 1, date('2022-06-01')) then 1 else 0 end as Fixed_PRMonth
FROM weeklyTable f left join InactiveUsersMonth i on f.Fixed_Account=i.Fixed_Account and Fixed_Month=ExitMonth
)
where RejoinerPopFlag=1 and Fixed_PRMonth=1 and Fixed_Month<>DATE('2022-06-01')
group by 1,2,3,4,5
)
,FullBase_Rejoiners AS(
SELECT distinct f.*,Fixed_PRMonth
,CASE WHEN Fixed_PRMonth=1 AND MainMovement_adj='5.Come Back to Life'
THEN 1 ELSE 0 END AS Fixed_RejoinerMonth
FROM weeklyTable f LEFT JOIN RejoinerMonthPopulation r ON f.Fixed_Account=r.Fixed_Account AND f.Fixed_Month=CAST(r.Month AS DATE)
where f.fixed_month=date('2022-06-01') --and f.fixed_account in(select fixed_account from InactiveUsersMonth)
order by 1,3,2 
)
select distinct fixed_month,mainmovement_adj,rejoinerflag,Fixed_RejoinerMonth,count(distinct fixed_account)
from FullBase_Rejoiners
group by 1,2,3,4 order by 1,2,3,4
/*
select distinct fixed_month,fixed_monthweek,count(distinct fixed_account)
from FullBase_Rejoiners
where Fixed_RejoinerMonth=1
group by 1,2 order by 1,2
*/
,fmc_table as(
select distinct * --fixed_month,fixed_account
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
where month=date(dt) --and fixed_account='104317400000' order by 1
and fixed_month=date('2022-06-01') and Fixed_RejoinerMonth=0
)
/*
select distinct n.fixed_month,n.fixed_monthweek,n.fixed_account,n.activebom,n.activeeom,n.b_active_flag,n.e_active_flag,n.mainmovement,n.Fixed_PRMonth,n.Fixed_RejoinerMonth,f.Fixed_RejoinerMonth
--fixed_month,fixed_account
from fullbase_rejoiners n inner join fmc_table f on n.fixed_account=f.fixed_account
where n.fixed_month=date('2022-06-01') and n.Fixed_RejoinerMonth=1
--group by 1,2,3 order by 1,2,3
order by 3,2
*/
------------------Churn atypical Flags -----------------------------------------------------------

,panel_so as (
    select account_id, order_id,
    case when max(lob_vo_count)> 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end as vol_lob_vo_count, 
    case when max(lob_bb_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_bb_count, 
    case when max(lob_tv_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_tv_count, 
    DATE_TRUNC('month', completed_date) as completed_month, completed_date,
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type,
    lob_VO_count, lob_BB_count, lob_TV_count, customer_id
    from (
        select * FROM "db-stage-dev"."so_hdr_cwc"
    WHERE org_cntry = 'Jamaica'
        AND (cease_reason_group in ('Voluntary', 'Customer Service Transaction', 'Involuntary') or cease_reason_group is null)
        AND (network_type NOT IN ('LTE','MOBILE') or network_type is null)
        AND order_status = 'COMPLETED' AND account_type = 'Residential')
        --AND order_type = 'DEACTIVATION'
        --AND DATE_TRUNC('month', completed_date) = ( select month_analysis from parameters))
    group by account_id, order_id, lob_vo_count, lob_bb_count, lob_tv_count, DATE_TRUNC('month', completed_date), completed_date, customer_id,
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type
    --order by completed_month, account_id, order_id
    )

,SO_LLAFlags AS(
 select completed_month, account_id, fixed_monthweek as week,
   sum(vol_lob_vo_count) + sum(vol_lob_bb_count) + sum(vol_lob_tv_count) --+ sum(vol_lob_other_count) 
   as vol_churn_rgu,
    case when sum(case when cease_reason_group = 'Customer Service Transaction' OR (cease_reason_group is null) then 1 else 0 end) > 0 then 1 else 0 end as cst_churn_flag,
    case when sum(case when cease_reason_group = 'Involuntary' then 1 else 0 end) > 0 then 1 else 0 end as non_pay_so_flag
    from panel_so p inner join WeeklyTable w on cast(p.account_id as varchar) = cast(w.fixed_account as varchar) and p.completed_month = w.fixed_month
    where completed_date > w.BOW and (completed_date <= w.EOW or EOW is null)
    group by account_id, completed_month, 3
)

,join_so_fixedbase as (
    select a.*, b.cst_churn_flag,
    case when a.MainChurnFlag ='Voluntary' then 'Voluntary' 
    when a.MainChurnFlag = 'Early Rejoiner' then 'Early Rejoiner'
    when b.cst_churn_flag = 1 and ((a.MainChurnFlag <>'Involuntary' and a.MainChurnFlag <> 'Voluntary') or a.MainChurnFlag is null) AND B_NumRGUs > E_Numrgus then 'CST Churner'
    when a.MainChurnFlag ='Involuntary' then 'Involuntary'
    when a.MainChurnFlag is null and ActiveEOM = 0 and cast(a.B_OutstAge as integer) <90 and (b.cst_churn_flag = 0 or b.cst_churn_flag is null) then 'Early Dx'
    --and ((length(a.fixed_account) = 12) OR (b.non_pay_so_flag = 1 AND length(a.fixed_account) = 8)) 
    when (a.mainmovement = '6.Null last day' and a.MainChurnFlag is null) and ((B_outstage is null and length(a.fixed_account) = 12) or ((b.non_pay_so_flag = 0 or b.non_pay_so_flag is null) AND length(a.fixed_account) = 8)) then 'Incomplete CST'
    when a.mainmovement = '2.Upsell' then 'Upsell'
    --when a.mainmovement = '4.New Customer' or (a.mainmovement = '5.Come Back to Life' and (a.mainchurnflag <> 'Early Rejoiner' or a.mainchurnflag is null)) then 'Gross/add rejoiner'
    when a.mainmovement_adj='5.Come Back to Life' and Fixed_RejoinerMonth=1 /*and a.mainchurnflag<> 'Early Rejoiner'*/ then 'Rejoiner'
    when a.mainmovement = '4.New Customer' or (a.mainmovement = '5.Come Back to Life' and (a.mainchurnflag <> 'Early Rejoiner' or a.mainchurnflag is null) or Fixed_RejoinerMonth=0) then 'Gross Add' --revisar
    end as FinalFixedChurnFlag
    from FullBase_Rejoiners a left join SO_LLAFlags b
    on cast(a.fixed_account as varchar) = cast(b.account_id as varchar)
    and a.fixed_month = b.completed_month and a.fixed_monthweek = b.week
   -- where atypical_churndate > a.BOW and (atypical_churndate <= a.EOW or EOW is null)
    )
    
/*
Select Fixed_month, fixed_monthweek, FinalFixedChurnFlag, 
case when FinalFixedChurnFlag = 'Early Rejoiner' then count(distinct fixed_account)*-1
else count(distinct fixed_account) end as Customers,
case when FinalFixedChurnFlag = 'Early Rejoiner' then sum(b_numrgus*-1) 
when FinalFixedChurnFlag = 'CST Churner' then sum(Dif_RGUs) 
when FinalFixedChurnFlag = 'Gross/add rejoiner' then sum(e_numrgus)
when FinalFixedChurnFlag = 'Upsell' then sum(dif_rgus)*-1
else sum(b_numrgus) end as RGUs
from join_so_fixedbase
where fixed_month = date('2022-06-01')  
group by 1,2,3
order by 1,2,3
*/
select distinct fixed_month,FinalFixedChurnFlag,Fixed_RejoinerMonth,count(distinct fixed_account)
from join_so_fixedbase
where fixed_month = date('2022-06-01') 
group by 1,2,3 order by 1,2,3
