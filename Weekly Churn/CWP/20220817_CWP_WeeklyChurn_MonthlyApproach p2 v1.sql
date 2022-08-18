with weeklytable as(
select fixed_month,fixed_monthweek,fixed_account,f_contactphone3,activebom as activebow,activeeom as activeeow,b_active_flag,e_active_flag,bow,b_date,b_tech_type,b_mixcode,b_mixcode_adj,b_mixname,b_mixname_adj,b_prodbbname,b_prodtvname,b_prodvoname,bb_rgu_bom,tv_rgu_bom,vo_rgu_bom,b_numrgus,b_bundlecode,b_bundlename,b_mrc,b_outstage,b_mrcbb,b_mrctv,b_mrcvo,b_maxstart,b_tenuredays,b_fixedtenuresegment,eow,e_date,e_tech_type,e_mixcode,e_mixcode_adj,e_mixname,e_mixname_adj,e_prodbbname,e_prodtvname,e_prodvoname,bb_rgu_eom,tv_rgu_eom,vo_rgu_eom,e_numrgus,e_bundlecode,e_bundlename,e_mrc,e_outstage,e_mrcbb,e_mrctv,e_mrcvo,e_maxstart,e_tenuredays,e_fixedtenuresegment,mrcdiff,last_rgus,dif_rgus,mainmovement,fixed_churntype,rejoinerflag,mainchurnflag,first_value(b_active_flag_adj) over(partition by fixed_account,fixed_month order by fixed_monthweek asc) as b_active_temp,first_value(e_active_flag_adj) over(partition by fixed_account,fixed_month order by fixed_monthweek desc) as e_active_temp
,first_value(b_numrgus) over(partition by fixed_account,fixed_month order by fixed_monthweek asc) as first_rgus_month,first_value(e_numrgus) over(partition by fixed_account,fixed_month order by fixed_monthweek desc) as last_rgus_month
,first_value(activebom) over(partition by fixed_account,fixed_month order by fixed_monthweek asc) as activebom_month,first_value(activeeom) over(partition by fixed_account,fixed_month order by fixed_monthweek desc) as activeeom_month
from (select *,case when b_active_flag is null then 'Inactive' when b_active_flag='DRC' then 'Inactive' else b_active_flag end as b_active_flag_adj,case when e_active_flag is null then 'Inactive' when e_active_flag='DRC' then 'Inactive' else e_active_flag end as e_active_flag_adj from  "lla_cco_int_san"."cwp_fix_stg_weekly_churn_input_mayjune")
)
------------------------Rejoiners-----------------------------------------------------------------
,InactiveUsersMonth AS (
select distinct Fixed_Month AS ExitMonth, Fixed_Account,DATE_ADD('MONTH', 1, Fixed_Month) AS RejoinerMonth
from weeklyTable
where b_active_temp='Active' and e_active_temp='Inactive'
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
,CASE WHEN Fixed_PRMonth=1 AND MainMovement='5.Come Back to Life'
THEN 1 ELSE 0 END AS Fixed_RejoinerMonth,
COALESCE(first_rgus_month,0) - COALESCE(last_rgus_month,0) as Dif_RGUsMonth
FROM weeklyTable f LEFT JOIN RejoinerMonthPopulation r ON f.Fixed_Account=r.Fixed_Account AND f.Fixed_Month=CAST(r.Month AS DATE)
--where f.fixed_month=date('2022-06-01') --and f.fixed_account in(select fixed_account from InactiveUsersMonth)
--order by 1,3,2 
)

------------------Churn atypical Flags -----------------------------------------------------------

,panel_so as (
    select account_id, order_id,
    case when max(lob_vo_count)> 0 and max(dxtype) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end as vol_lob_vo_count, 
    case when max(lob_bb_count) > 0 and max(dxtype) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_bb_count, 
    case when max(lob_tv_count) > 0 and max(dxtype) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_tv_count, 
    DATE_TRUNC('month', completed_date) as completed_month, completed_date,
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type,
    lob_VO_count, lob_BB_count, lob_TV_count, customer_id,dxtype
    from (select *,CASE WHEN cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91') THEN 'Voluntario'
        WHEN cease_reason_code IN('2','74') THEN 'Involuntario'
        WHEN (cease_reason_code = '9' AND cease_reason_desc='CAMBIO DE TECNOLOGIA') OR (cease_reason_code IN('32','44','55','72')) THEN 'Migracion'
        WHEN cease_reason_code = '9' AND cease_reason_desc<>'CAMBIO DE TECNOLOGIA' THEN 'Voluntario'
        ELSE NULL END AS DxType 
        from "db-stage-dev"."so_hdr_cwp" where order_type = 'DEACTIVATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
        )
    where dxtype in('Voluntary', 'Migracion', 'Involuntary') or dxtype is null
    group by account_id, order_id, lob_vo_count, lob_bb_count, lob_tv_count, DATE_TRUNC('month', completed_date), completed_date, customer_id,
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type,dxtype
    )

,SO_LLAFlags AS(
 select completed_month, account_id, fixed_monthweek as week,
   sum(vol_lob_vo_count) + sum(vol_lob_bb_count) + sum(vol_lob_tv_count) --+ sum(vol_lob_other_count) 
   as vol_churn_rgu,
    case when sum(case when dxtype = 'Migracion'  then 1 else 0 end) > 0 then 1 else 0 end as cst_churn_flag,
    case when sum(case when dxtype = 'Involuntary' then 1 else 0 end) > 0 then 1 else 0 end as non_pay_so_flag
    from panel_so p inner join WeeklyTable w on cast(p.account_id as varchar) = cast(w.fixed_account as varchar) and p.completed_month = w.fixed_month
    where completed_date > w.BOW and (completed_date <= w.EOW or EOW is null)
    group by account_id, completed_month, 3
)

,join_so_fixedbase as (
    select a.*, b.cst_churn_flag,
    case when a.MainChurnFlag ='Voluntary' and coalesce(last_rgus_month,0) <= coalesce(first_rgus_month,0) and b_active_temp = 'Active' then 'Voluntary' 
     when a.MainChurnFlag is null and b_active_temp = 'Active' and e_active_temp = 'Inactive' and cast(a.B_OutstAge as integer) <90 and non_pay_so_flag=1 then 'Early Dx'
    when a.MainChurnFlag ='Involuntary' and b_active_flag = 'Active' then 'Involuntary'
    when a.MainChurnFlag = 'Early Rejoiner' then 'Early Rejoiner'
    when ((b.cst_churn_flag = 1 and ((a.MainChurnFlag <>'Involuntary' and a.MainChurnFlag <> 'Voluntary') or a.MainChurnFlag is null)) ) and b_active_temp = 'Active' and e_active_temp = 'Inactive' then 'Incomplete CST'
    when a.mainmovement = '2.Upsell' and last_rgus_month > first_rgus_month and b_active_flag = 'Active' then 'Upsell'
    when a.mainmovement='5.Come Back to Life' and Fixed_RejoinerMonth=1 and (a.MainChurnFlag<>'Early Rejoiner' or a.MainChurnFlag is null) then 'Rejoiner'
    when a.mainmovement = '4.New Customer' or (a.mainmovement = '5.Come Back to Life' and (a.mainchurnflag <> 'Early Rejoiner' or a.mainchurnflag is null) and Fixed_RejoinerMonth=0) and e_active_temp = 'Active' and b_active_temp = 'Inactive' then 'Gross Add' --revisar
    end as FinalFixedChurnFlag
    from FullBase_Rejoiners a left join SO_LLAFlags b
    on cast(a.fixed_account as varchar) = cast(b.account_id as varchar)
    and a.fixed_month = b.completed_month and a.fixed_monthweek = b.week
    )
    
, finalfixedbase as(
select a.*,
case when lag(finalfixedchurnflag,1)  over(partition by fixed_account,fixed_month order by fixed_monthweek asc) is null and finalfixedchurnflag = 'CST Churner' then 'CST Churner'
when finalfixedchurnflag <> 'CST Churner' then finalfixedchurnflag
else null
end as finalfixedchurnflag_adj
,first_value(b_active_flag) over(partition by fixed_account,fixed_month order by fixed_monthweek asc) as first_active_flag
from join_so_fixedbase a
)
,voluntary as(
select *
from finalfixedbase
where finalfixedchurnflag_adj='Voluntary' and fixed_month=date('2022-06-01'))
,fmc_table as(
select *
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where month=date(dt) and fixedchurnsubtype='Voluntary' and month=date('2022-06-01') --and fixedaccount not in(select fixed_account from voluntary)
)
select *
from finalfixedbase 
where finalfixedchurnflag_adj='Voluntary' and fixed_account not in(select fixedaccount from fmc_table)
order by 3,1,2
