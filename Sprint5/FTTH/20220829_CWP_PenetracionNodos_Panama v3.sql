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
,join_dna as(
select distinct p.*
from fmc_table f inner join ftth_accounts_month_adj p on f.finalaccount=cast(p.acct_no as varchar) and f.month=p.month_adj
)
,coordenates as(
select distinct provincia,distrito,corregimiento,first_value(longitude) over(partition by provincia,distrito,corregimiento order by latitude) as longitude,first_value(latitude) over(partition by provincia,distrito,corregimiento order by longitude) as latitude
from (SELECT distinct provincia,distrito,case when distrito='Panama' and corregimiento like '%anitas%' then 'Las Mañanitas' when corregimiento='Betania' then 'Bethania' when (distrito='Panama' and corregimiento='Parque Lefebre') then 'Parque Lefevre' else corregimiento end as corregimiento,longitude,latitude
FROM "lla_cco_int_san"."cwp_ext_corregimientos_ftth" )
)
,ftth_project as(
select *,first_value(fecha_entrega_all) over(partition by nodo,dt order by dt asc) as fecha_entrega
from(select *, trim(b."nodo/fdh") as nodo,Tipo as Tech,b."home passed" as Home_Passed,b."velocidad máxima (coaxial)" as velocidad
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(provincia), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as provincias
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(DISTRITO), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as distritos
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(corregimiento), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as corregimientos
,date(date_parse(concat(cast("año" as varchar),lpad(cast(mes as varchar),2,'0'),'01'),'%Y%m%d')) as fecha_entrega_all
FROM "lla_cco_int_san"."cwp_ext_ftth" b
where provincia='Panamá' and b."velocidad máxima (coaxial)"=1000)
)
,count_districts as(
select distinct dt,provincias,distritos,nodo,count(distinct corregimientos) as cont
from ftth_project
group by 1,2,3,4 order by 1,4,cont desc
)
,ftth_project_adj as(
select distinct *,first_value(corregimientos) over(partition by dt,distritos,nodo order by home_passed desc) as corregimiento_adj
from(select distinct f.dt,f.fecha_entrega,f.provincias,f.distritos,f.corregimientos,f.nodo,cont,sum(f.home_passed) as home_passed
from ftth_project f left join count_districts c on f.dt=c.dt and f.provincias=c.provincias and f.nodo=c.nodo
--where velocidad=1000 
--where date(f.dt)=date('2022-05-01') and cont>1
group by 1,2,3,4,5,6,7) order by 1,5
)
,ftth_join_coord as (
select distinct dt,provincias as provincia,distritos as distrito,corregimiento_ADJ as corregimiento,nodo,home_passed,longitude,latitude,fecha_entrega
from ftth_project_adj f left join coordenates c on f.provincias=lower(c.provincia) and f.distritos=lower(c.distrito) and f.corregimiento_adj=lower(c.corregimiento)
)
,penetration_fields as(
select distinct date(b.dt) as Month,fecha_entrega,Provincia,Distrito,Corregimiento,acct_no,nodo,sum(home_passed) as home_passed
,case when distrito='chepo' and corregimiento='chepo' then -79.07712291364297 when corregimiento='caimitillo' then -79.540962 when corregimiento='don bosco' then -79.417167 else longitude end as longitude
,case when distrito='chepo' and corregimiento='chepo' then 9.143327252649252 when corregimiento='caimitillo' then 9.166807 when corregimiento='don bosco' then 9.05325 else latitude end as latitude
from ftth_join_coord b left join join_dna a on b.nodo=a.trim_fdh and date(b.dt)=month_adj
group by 1,2,3,4,5,6,7,longitude,latitude
)
,initial_grouping as(
select distinct Month,fecha_entrega,Provincia,Distrito,Corregimiento,longitude,latitude,Nodo,Home_Passed,count(distinct acct_no) as Active_Users
from penetration_fields
--where month=date('2022-05-01') and nodo='ABG-001'
group by 1,2,3,4,5,6,7,8,9
)
select distinct Month,fecha_entrega as Cohort_Month,Provincia,Distrito,Corregimiento,longitude,latitude,Nodo,sum(Home_Passed) as Home_Passed,Active_Users,Active_Users*100/sum(home_passed) as P
from initial_grouping
where month=date('2022-05-01') --and nodo='NSG-001'
group by 1,2,3,4,5,6,7,8,active_users
order by 1,nodo,2,3,4
