/* 
Penetracion reporte oficial:
Tech, Provincia, Nodo/ FDH,	Casas pasdas, Clientes activos,	%Penetración
*/
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
FROM "lla_cco_int_san"."cwp_con_ext_ftth_ad"
--WHERE LENGTH(ACCT_NO) = 12
)
,join_dna as(
select distinct p.*
from fmc_table f inner join ftth_accounts_month_adj p on f.finalaccount=cast(p.acct_no as varchar) and f.month=p.month_adj
)
,ftth_project as(
select trim(b."nodo/fdh") as nodo,Tipo as Tech,b."home passed" as Home_Passed,b."velocidad máxima (coaxial)" as velocidad,*
FROM "lla_cco_int_san"."cwp_ext_ftth" b
)
,penetration_fields as(
select distinct date(b.dt) as Month,Provincia,Distrito,Corregimiento,acct_no,nodo,tech,sum(home_passed) as home_passed,velocidad
from ftth_project b left join join_dna a on b.nodo=a.trim_fdh and date(b.dt)=month_adj
where velocidad=1000
group by 1,2,3,4,5,6,7,velocidad
)
,initial_grouping as(
select distinct Month,Provincia,Distrito,Corregimiento,Nodo,Home_Passed,count(distinct acct_no) as Active_Users
from penetration_fields
--where month=date('2022-05-01') and nodo='ABG-001'
group by 1,2,3,4,5,6
)
,final_grouping as(
select distinct Month,Provincia,Distrito,Corregimiento,Nodo,sum(Home_Passed) as Home_Passed,Active_Users,Active_Users*100/sum(home_passed)
from initial_grouping
where month=date('2022-05-01') 
group by 1,2,3,4,5,active_users
order by 1,3,2
)
select distinct Month,Provincia,Distrito,Corregimiento,sum(home_passed),sum(active_users),round(sum(cast(active_users as double))*100/sum(cast(home_passed as double)),2)
from final_grouping
group by 1,2,3,4
order by 1,2,3,4
