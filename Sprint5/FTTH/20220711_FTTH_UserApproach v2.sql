with
FMC_Table AS ( 
SELECT * FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
where month=date(dt)
)
,ftth_accounts_month_adj as(
select distinct case month
                     when 20220201 then date('2022-02-01')
                     when 20220301 then date('2022-03-01')
                     when 20220401 then date('2022-04-01')
                     when 20220501 then date('2022-05-01')
                end as Month_Adj
,trim(building_fdh) as trim_fdh,*
FROM "lla_cco_int_ext"."cwp_con_ext_ftth_ad"
)
,ftth_project as(
select trim(b."nodo/fdh") as nodo,Tipo as Tech,b."home passed" as Home_Passed,b."velocidad máxima (coaxial)" as velocidad,*
FROM "lla_cco_int_ext"."cwp_ext_ftth" b
)
,consolidated_ftth as(
select distinct date(b.dt) as Month,Provincia,acct_no,nodo,tech,sum(home_passed) as home_passed,velocidad
from ftth_accounts_month_adj a right join ftth_project b on b.nodo=a.trim_fdh and date(b.dt)=month_adj
where velocidad=1000
group by 1,2,3,4,5,7
)
,ftth_flags as(
select distinct f.*
,case when acct_no is not null then 'FTTH User' 
      when acct_no is null and (b_bb is not null or e_bb is not null) then 'Other BB User'
      when acct_no is null and b_bb is null and e_bb is null then 'Other User'
else null end as FTTH_User
,provincia,nodo,home_passed
,case when acct_no is not null and concat(b_final_techflag,e_final_techflag) NOT LIKE '%FTTH%' then 1 else 0 end as Atypical_FTTH
,case when b_bb is not null or e_bb is not null then 1 else 0 end as BB_User
from fmc_table f left join consolidated_ftth p on f.finalaccount=cast(p.acct_no as varchar) and f.month=p.month
)
select distinct month,final_bom_activeflag,final_eom_activeflag,f_activebom,f_activeeom,sum(cast(round(b_fixed_mrc,0) as int)) as b_fixed_mrc,b_mixname_adj,b_mixcode_adj,sum(cast(round(e_fixed_mrc,0) as int)) as e_fixed_mrc,e_mixname_adj,e_mixcode_adj,fixedmainmovement,fixedspinmovement,fixedchurnflag,fixedchurntype,fixedchurnsubtype,finalchurnflag,churntypefinalflag,b_fmctype,e_fmctype,b_fmcsegment,e_fmcsegment,sum(cast(round(b_total_mrc,0) as int)) as b_total_mrc,sum(cast(round(e_total_mrc,0) as int)) as e_total_mrc,partial_total_churnflag,waterfall_flag,b_final_techflag,e_final_techflag,ftth_user,BB_User,Atypical_FTTH
,count(distinct finalaccount) as TotalAccounts,count(distinct Fixedaccount) as FixedAccounts
,b_numrgus,e_numrgus,b_totalrgus,e_totalrgus
from ftth_flags
--where nodo is not null
group by 1,2,3,4,5,7,8,10,11,12,13,14,15,16,17,18,19,20,21,22,25,26,27,28,29,30,31,34,35,36,37
--order by 1,2,3
