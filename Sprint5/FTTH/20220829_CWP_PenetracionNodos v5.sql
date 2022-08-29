with
FMC_Table AS ( 
SELECT * FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
where month=date(dt)
)
,ftth_accounts_month_adj as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as Month_Adj
,trim(building_fdh) as trim_fdh,*
FROM "lla_cco_int_san"."cwp_con_ext_ftth_ad"
--WHERE LENGTH(ACCT_NO) = 12
)
,join_dna as(
select distinct p.*
from fmc_table f inner join ftth_accounts_month_adj p on f.finalaccount=cast(p.acct_no as varchar) and f.month=p.month_adj
)
,coordenates as(
SELECT distinct provincia,case when provincia='Veraguas' and distrito like '%azas%' then 'Cañazas' when provincia='Herrera' and distrito like '%Pesu%' then 'Pese' else distrito end as distrito,longitude,latitude
FROM "lla_cco_int_san"."cwp_con_ext_coord"
)
,ftth_project as(
select *,first_value(fecha_entrega_all) over(partition by nodo,dt order by dt) as fecha_entrega
from(select *, trim(b."nodo/fdh") as nodo,Tipo as Tech,b."home passed" as Home_Passed,b."velocidad máxima (coaxial)" as velocidad
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(provincia), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as provincias
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(DISTRITO), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as distritos
,date(date_parse(concat(cast("año" as varchar),lpad(cast(mes as varchar),2,'0'),'01'),'%Y%m%d')) as fecha_entrega_all
FROM "lla_cco_int_san"."cwp_ext_ftth" b)
)
,count_districts as(
select distinct dt,provincias,nodo,count(distinct distritos) as cont
from ftth_project
group by 1,2,3
)
,ftth_project_adj as(
select distinct *,first_value(distritos) over(partition by dt,nodo order by home_passed desc) as distrito_adj
from(select distinct f.dt,f.provincias,f.distritos,f.nodo,f.fecha_entrega,cont,sum(f.home_passed) as home_passed
from ftth_project f left join count_districts c on f.dt=c.dt and f.provincias=c.provincias and f.nodo=c.nodo
where velocidad=1000 --and cont>1
group by 1,2,3,4,5,6)
)
,ftth_join_coord as (
select distinct dt,provincia,distrito,nodo,home_passed,longitude,latitude,fecha_entrega
from ftth_project_adj f left join coordenates c on f.provincias=lower(c.provincia) and f.distrito_adj=lower(c.distrito)
)
,penetration_fields as(
select distinct date(b.dt) as Month,fecha_entrega,Provincia,Distrito,acct_no,nodo,sum(home_passed) as home_passed,longitude,latitude
from ftth_join_coord b left join join_dna a on b.nodo=a.trim_fdh and date(b.dt)=month_adj
--where velocidad=1000
group by 1,2,3,4,5,6,longitude,latitude
)
,initial_grouping as(
select distinct Month,fecha_entrega,Provincia,Distrito,longitude,latitude,Nodo,Home_Passed,count(distinct acct_no) as Active_Users
from penetration_fields
--where month=date('2022-05-01') and nodo='ABG-001'
group by 1,2,3,4,5,6,7,8
)

select distinct Month,fecha_entrega as Cohort_Month,Provincia,Distrito,longitude,latitude,Nodo,sum(Home_Passed) as Home_Passed,Active_Users,Active_Users*100/sum(home_passed) as P
from initial_grouping
--where month=date('2022-05-01') --and nodo='NSG-001'
group by 1,2,3,4,5,6,7,active_users
order by 1,7,3
