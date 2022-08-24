--- Gross Adds and rejoiners------
SELECT month, sum(e_numrgus)
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
WHERE MONTH = DATE(DT) and (fixedmainmovement = '4.New Customer' or fixedmainmovement = '5.Come Back to Life') 
group by 1
order by 1;

-------Upsell-------

SELECT month, sum(e_numrgus) - sum(b_numrgus)
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
WHERE MONTH = DATE(DT) and fixedmainmovement = '2.Upsell'
group by 1
order by 1;


-----Downsell-----
SELECT month, sum(COALESCE(B_NUMRGUS,0) - coalesce(E_numrgus,0)) as donwsellrgus
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where fixedmainmovement = '3.Downsell'  and MONTH = DATE(DT) 
group by 1
order by 1;


----- Churn-----

SELECT month, sum(b_numrgus)
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where fixedchurntype is not null and MONTH = DATE(DT) 
group by 1
order by 1;
