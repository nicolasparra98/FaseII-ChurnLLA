with
ftth_project as(
select *, trim(b."nodo/fdh") as nodo,Tipo as Tech,b."home passed" as Home_Passed,b."velocidad máxima (coaxial)" as velocidad
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(provincia), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as provincias
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(DISTRITO), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as distritos
FROM "lla_cco_int_san"."cwp_ext_ftth" b
)
,conteo as (
select distinct dt,provincias,nodo,count(distinct distritos) as cont
from ftth_project
group by 1,2,3
order by 1,4 desc,2,3
)
select distinct *,first_value(distritos) over(partition by dt,nodo order by home_passed desc) as distrito_adj
from(select distinct f.dt,f.provincias,f.distritos,f.nodo,cont,sum(f.home_passed) as home_passed
from ftth_project f left join conteo c on f.dt=c.dt and f.provincias=c.provincias and f.nodo=c.nodo
where velocidad=1000 and cont>1
group by 1,2,3,4,5)
order by 1,4,2,3
