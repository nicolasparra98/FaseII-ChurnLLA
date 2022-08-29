with
FMC_Table AS ( 
SELECT * FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
where month=date(dt)
)
,ftth_accounts_month_adj as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as Month_Adj
,trim(building_fdh) as trim_fdh,*
FROM "lla_cco_int_san"."cwp_con_ext_ftth_ad"
)
,ftth_project as(
select *,first_value(fecha_entrega_all) over(partition by nodo,dt order by dt) as fecha_entrega
from(select trim(b."nodo/fdh") as nodo,Tipo as Tech,b."home passed" as Home_Passed,b."velocidad máxima (coaxial)" as velocidad,date(date_parse(concat(cast("año" as varchar),lpad(cast(mes as varchar),2,'0'),'01'),'%Y%m%d')) as fecha_entrega_all,*
FROM "lla_cco_int_san"."cwp_ext_ftth" b)
)
,consolidated_ftth as(
select distinct date(b.dt) as Month,Provincia,distrito,corregimiento,acct_no,nodo,tech,fecha_entrega,sum(home_passed) as home_passed,velocidad
from ftth_accounts_month_adj a right join ftth_project b on b.nodo=a.trim_fdh and date(b.dt)=month_adj
where velocidad=1000
group by 1,2,3,4,5,6,7,8,10
)
,ftth_flags as(
select distinct f.*
,case when acct_no is not null then 'FTTH User' 
      when acct_no is null and (b_bb is not null or e_bb is not null) then 'Other BB User'
      when acct_no is null and b_bb is null and e_bb is null then 'Other User'
else null end as FTTH_User
,provincia,distrito,corregimiento,nodo,home_passed,fecha_entrega
,case when acct_no is not null and concat(coalesce(b_final_techflag,''),coalesce(e_final_techflag,'')) NOT LIKE '%FTTH%' then 1 else 0 end as Atypical_FTTH
,case when b_bb is not null or e_bb is not null then 1 else 0 end as BB_User
from fmc_table f left join consolidated_ftth p on f.finalaccount=cast(p.acct_no as varchar) and f.month=p.month
)

select distinct month,provincia,distrito,corregimiento,fixedaccount,nodo as fdh,fecha_entrega as Month_Cohort,final_bom_activeflag,final_eom_activeflag,f_activebom,f_activeeom,fixedmainmovement,fixedspinmovement,fixedchurntype,fixedchurnsubtype,finalchurnflag,churntypefinalflag,b_final_tenure,e_final_tenure,b_fmctype,e_fmctype,b_fmcsegment,e_fmcsegment,b_total_mrc,e_total_mrc,partial_total_churnflag,waterfall_flag,b_final_techflag,e_final_techflag,downsell_split,downspin_split,Atypical_FTTH
from ftth_flags
where nodo is not null --and atypical_ftth=1
--group by 1,2,3,4,5,7,8,10,11,12,13,14,15,16,17,18,19,20,21,22,25,26,27,28,29,30,31,34,35,36,37
--order by 1,2,3
--,b_numrgus,e_numrgus,b_totalrgus,e_totalrgus,
