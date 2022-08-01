SELECT month,e_fmcsegment, count(distinct finalaccount) as final_account,count(distinct fixedaccount) as fixed_account,count(distinct mobile_account) as mobile_account
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where month=date(dt)
group by 1,2 order by 1,2

SELECT distinct month,e_fmcsegment, sum(finalaccount) as final_account,sum(fixedaccount) as fixed_account,sum(mobile_account) as mobile_account
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_group_prod"  --limit 10
where month=date(dt)
group by 1,2 order by 1,2
