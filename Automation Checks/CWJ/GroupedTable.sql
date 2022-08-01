SELECT distinct month,E_FMC_SEGMENT,sum(numaccounts) as total,sum(numfixed) as fixed,sum(nummobile) as mobile
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_group_prod" 
where month=date(dt) 
group by 1,2 order by 1,2
--fixedchurntypeflag

select distinct month,
E_FMC_SEGMENT,count(distinct final_account),count(distinct fixed_account),count(distinct mobile_account)
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" 
where month=date(dt)
group by 1,2 order by 1,2
