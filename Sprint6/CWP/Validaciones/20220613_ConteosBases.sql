--select distinct *
--from "lla_cco_int_ana"."cwp_kpibasetable"
--limit 10
/*
select distinct month,b_final_techflag--,finalchurnflag,churntypefinalflag,Partial_Total_Churnflag,e_final_techflag
,count(distinct finalaccount) as Users
from "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
--"lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
where month=date('2022-05-01') and final_bom_activeflag = 1
group by 1,2--,3,4,5,6
order by 1,2--,3,4,5,6
*/
SELECT distinct month,b_final_techflag,churntypefinalflag,count(distinct finalaccount)
from "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
where month=date('2022-05-01') and 
finalchurnflag<>'Non Churner' and partial_total_churnflag='Total Churner'
group by 1,2,3
