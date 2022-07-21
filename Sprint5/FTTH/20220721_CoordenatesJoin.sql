with
coordenates as(
SELECT distinct provincia,case when provincia='Veraguas' and distrito like '%azas%' then 'Cañazas' when provincia='Herrera' and distrito like '%Pesu%' then 'Pese' else distrito end as distrito,longitude,latitude
FROM "lla_cco_int_san"."cwp_con_ext_coord"
)
,ftth_info as(
SELECT distinct f."nodo/fdh", f."home passed",f."velocidad máxima (coaxial)"
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(provincia), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as provincia
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(DISTRITO), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as distrito
FROM "lla_cco_int_san"."cwp_ext_ftth" f
)
,prueba as(
select distinct f."nodo/fdh", f."home passed",f."velocidad máxima (coaxial)",f.provincia,f.distrito,longitude,latitude
from ftth_info f left join coordenates c on f.provincia=lower(c.provincia) and f.distrito=lower(c.distrito)
)
select distinct provincia,distrito,longitude,latitude
from prueba
