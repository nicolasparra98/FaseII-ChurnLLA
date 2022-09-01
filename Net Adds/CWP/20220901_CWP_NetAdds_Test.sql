--"lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba1SEPT"
--"lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba1SEPT_abr"
/*
select distinct fixedmonth, sum(e_numrgus)
from "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba1SEPT_jul"
where (fixedmainmovement = '4.New Customer' or fixedmainmovement = '5.Come Back to Life') 
group by 1
order by 1
*/
/*
SELECT fixedmonth, sum(e_numrgus) - sum(b_numrgus)
FROM "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba1SEPT_jul"
WHERE fixedmainmovement = '2.Upsell'
group by 1
order by 1
*/
/*
SELECT fixedmonth, sum(COALESCE(B_NUMRGUS,0) - coalesce(E_numrgus,0)) as donwsellrgus
FROM "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba1SEPT_jul" 
where fixedmainmovement = '3.Downsell'  
group by 1
*/
--/*
SELECT fixedmonth, sum(b_numrgus)
FROM "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba1SEPT_jul" 
where fixedchurntype is not null 
group by 1
order by 1
--*/
