select distinct month
--,finalchurnflag
,churntypefinalflag
--,fixedchurntype
,count(distinct finalaccount)
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
where month=date(dt)
--and final_bom_activeflag=1
group by 1,2--,3
order by 1,2--,3

select distinct month
,count(distinct finalaccount)
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
where month=date(dt) and final_bom_activeflag=1
group by 1
order by 1
